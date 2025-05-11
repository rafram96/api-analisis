from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi import HTTPException
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import uvicorn
from collections import defaultdict
import boto3
import os
from datetime import datetime

app = FastAPI(title="SmartStock Analytics API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["smartstock_analytics"]
s3_client = boto3.client('s3')
BUCKET_NAME = 'proy-cloud-bucket'  # Tu bucket
CARPETA_DESTINO = 'Analisis/graficas/'  # Carpeta correcta que pediste

# Ruta al CSV local
CSV_LOCAL_PATH = "./movimiento_inventario.csv"


# ========= ENDPOINTS ==========
@app.get("/ventas/top", summary="Obtener datos de productos más vendidos")
def get_top_ventas(limit: int = 10):
    ventas = list(db.ventas_aggregadas.find().sort("total_ventas", -1).limit(limit))
    labels = [venta.get("nombre_producto", f"Producto {venta['producto_id']}") for venta in ventas]
    values = [venta["total_ventas"] for venta in ventas]
    return JSONResponse(content={"labels": labels, "values": values})


@app.get("/stock/alertas", summary="Obtener distribución de alertas de stock")
def get_alertas_stock():
    alertas = list(db.alertas_stock.find())
    conteo = {"CRÍTICO": 0, "BAJO": 0, "NORMAL": 0}
    for alerta in alertas:
        estado = alerta.get("estado", "NORMAL")
        conteo[estado] += 1
    labels = list(conteo.keys())
    values = list(conteo.values())
    return JSONResponse(content={"labels": labels, "values": values})


@app.get("/ventas/estacionalidad", summary="Obtener estacionalidad de ventas aleatoria")
def get_ventas_estacionalidad():
    # Pedimos 5 productos random directamente a MongoDB
    estacionalidad = list(db.estacionalidad.aggregate([{ "$sample": { "size": 5 } }]))

    if not estacionalidad:
        raise HTTPException(status_code=404, detail="No se encontraron productos aleatorios.")

    labels = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
              "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

    datasets = []
    for dato in estacionalidad:
        datasets.append({
            "label": dato.get("nombre_producto", "Producto Desconocido"),
            "data": [dato.get("ventas_por_mes", {}).get(mes, 0) for mes in labels]
        })

    return JSONResponse(content={"labels": labels, "datasets": datasets})


@app.post("/sync", summary="Sincronizar datos desde CSV local")
def sync_local_csv():
    try:
        df = pd.read_csv(CSV_LOCAL_PATH)

        ventas_totales = defaultdict(int)
        ventas_por_mes = defaultdict(lambda: defaultdict(int))
        stock_actual = defaultdict(int)

        for _, row in df.iterrows():
            producto_id = row['producto_id']
            nombre_producto = row.get('nombre_producto', f"Producto {producto_id}")
            tipo = row['tipo']
            cantidad = row['cantidad']
            fecha = row['fecha']

            if tipo == 'entrada':
                stock_actual[producto_id] += cantidad
            elif tipo == 'salida':
                stock_actual[producto_id] -= cantidad
                ventas_totales[producto_id] += cantidad
                if isinstance(fecha, str):
                    fecha = datetime.strptime(fecha, "%Y-%m-%d %H:%M:%S")
                mes = fecha.strftime("%B").lower()
                ventas_por_mes[producto_id][mes] += cantidad

        # Limpiar colecciones
        db.ventas_aggregadas.delete_many({})
        db.alertas_stock.delete_many({})
        db.estacionalidad.delete_many({})

        # Insertar datos nuevos
        for producto_id, total in ventas_totales.items():
            db.ventas_aggregadas.insert_one({
                "producto_id": int(producto_id),
                "nombre_producto": f"Producto {producto_id}",  # Asignamos un nombre base
                "total_ventas": int(total)
            })

        for producto_id, stock in stock_actual.items():
            estado = "CRÍTICO" if stock < 10 else "BAJO" if stock < 50 else "NORMAL"
            db.alertas_stock.insert_one({
                "producto_id": int(producto_id),
                "nombre_producto": f"Producto {producto_id}",
                "stock_actual": int(stock),
                "estado": estado
            })

        for producto_id, ventas_mes in ventas_por_mes.items():
            db.estacionalidad.insert_one({
                "producto_id": int(producto_id),
                "nombre_producto": f"Producto {producto_id}",
                "ventas_por_mes": dict(ventas_mes)
            })

        return JSONResponse(content={"message": "Sincronización local completada exitosamente."})

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)



@app.post("/graficas", summary="Generar gráficas y subirlas a S3")
def generar_y_subir_graficas():
    # 1. Generar las gráficas localmente
    graficar_top_ventas()
    graficar_alertas_stock()
    graficar_estacionalidad()

    # 2. Subir los PNG generados
    archivos = ["top_ventas.png", "alertas_stock.png", "estacionalidad_ventas.png"]
    urls_subidas = []

    for archivo in archivos:
        try:
            nombre_s3 = f"{CARPETA_DESTINO}{datetime.now().strftime('%Y%m%d_%H%M%S')}_{archivo}"
            s3_client.upload_file(
                Filename=archivo,
                Bucket=BUCKET_NAME,
                Key=nombre_s3,
                ExtraArgs={'ContentType': 'image/png', 'ACL': 'public-read'}
            )
            url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{nombre_s3}"
            urls_subidas.append(url)
        except Exception as e:
            return {"error": f"Error subiendo {archivo}: {str(e)}"}

    return {"graficas_subidas": urls_subidas}










# ========= FUNCIONES DE GRAFICACIÓN ==========
def graficar_top_ventas(limit=10):
    ventas = list(db.ventas_aggregadas.find().sort("total_ventas", -1).limit(limit))
    productos = [venta.get("nombre_producto", f"Producto {venta['producto_id']}") for venta in ventas]
    cantidades = [venta["total_ventas"] for venta in ventas]

    plt.figure(figsize=(12,6))
    sns.barplot(x=cantidades, y=productos, palette="viridis")
    plt.title("Top Productos Más Vendidos")
    plt.xlabel("Total de Ventas")
    plt.ylabel("Producto")
    plt.tight_layout()
    plt.savefig("top_ventas.png")
    plt.show()

def graficar_alertas_stock():
    alertas = list(db.alertas_stock.find())
    conteo = {"CRÍTICO": 0, "BAJO": 0, "NORMAL": 0}
    for alerta in alertas:
        estado = alerta.get("estado", "NORMAL")
        conteo[estado] += 1

    labels = list(conteo.keys())
    valores = list(conteo.values())

    plt.figure(figsize=(8,8))
    plt.pie(valores, labels=labels, autopct='%1.1f%%', startangle=140, colors=["#ff4d4d", "#ffcc00", "#66b266"])
    plt.title("Distribución de Estados de Stock")
    plt.axis("equal")
    plt.savefig("alertas_stock.png")
    plt.show()
def graficar_estacionalidad():
    estacionalidad = list(db.estacionalidad.find().limit(3))
    meses = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
             "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

    plt.figure(figsize=(12,6))

    for dato in estacionalidad:
        ventas = [dato["ventas_por_mes"].get(mes, 0) for mes in meses]
        plt.plot(meses, ventas, marker='o', label=dato.get("nombre_producto", f"Producto {dato['producto_id']}"))

    plt.title("Estacionalidad de Ventas (3 Productos)")
    plt.xlabel("Mes")
    plt.ylabel("Ventas")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig("estacionalidad_ventas.png")
    plt.show()


# ========= EJECUCIÓN DIRECTA ==========

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8082, reload=True)

