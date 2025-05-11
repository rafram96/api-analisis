# SmartStock Analytics API

## Estructura de Archivos
```
smartstock-api/
├── main.py
├── requirements.txt
├── Dockerfile
└── movimiento_inventario.csv
```

## Pasos para Dockerizar y Ejecutar

### 1. Preparar los archivos
Asegúrate de tener todos los archivos en un mismo directorio:
- `main.py` (código fuente de la API)
- `requirements.txt` (dependencias)
- `Dockerfile` (instrucciones para construir la imagen)
- `movimiento_inventario.csv` (datos de ejemplo)

### 2. Construir la imagen Docker
Puedes construir la imagen de dos maneras:

**Opción 1:** Usando Docker directamente
```bash
docker build -t smartstock-api-analytics .
```

**Opción 2:** Usando Docker Compose
```bash
docker-compose build
```

### 3. Ejecutar los servicios
**Usando Docker Compose** (método recomendado):
```bash
docker-compose up -d
```

Este comando iniciará tanto la API como la base de datos MongoDB en segundo plano.

### 4. Verificar el funcionamiento
Accede a la API en tu navegador o mediante herramientas como cURL o Postman:
```
http://localhost:8000/
```

### 5. Explorar los datos
Después de sincronizar, puedes acceder a las siguientes rutas:
- Productos más vendidos: `http://localhost:8082/ventas/top`
- Alertas de stock: `http://localhost:8082/stock/alertas`
- Estacionalidad de ventas: `http://localhost:8082/ventas/estacionalidad`





## Subir la imagen a Docker Hub (Opcional)

Para compartir tu imagen con otros o usarla en entornos diferentes:

1. Etiqueta tu imagen con tu nombre de usuario de Docker Hub:
```bash
docker tag smartstock-api tuusuario/smartstock-api:latest
```

2. Inicia sesión en Docker Hub:
```bash
docker login
```

3. Sube la imagen:
```bash
docker push tuusuario/smartstock-api:latest
```

4. Para usar la imagen desde Docker Hub en otro entorno, modifica `docker-compose.yml`:
```yaml
version: '3.8'

services:
  api:
    image: tuusuario/smartstock-api:latest
    ports:
      - "8000:8000"
    environment:
      - MONGO_URI=mongodb://mongodb:27017/
    depends_on:
      - mongodb
    networks:
      - smartstock-network

  # Resto del archivo igual...
```

5. Y luego simplemente ejecuta:
```bash
docker-compose up -d
```

## Solución de problemas comunes

1. **No se puede conectar a MongoDB**: Verifica que el servicio de MongoDB esté ejecutándose correctamente con `docker-compose ps`.

2. **El archivo CSV no se carga**: Comprueba que el archivo `movimiento_inventario.csv` esté en el mismo directorio que `main.py` dentro del contenedor. Puedes verificarlo con:
```bash
docker exec -it smartstock-api_api_1 ls -la
```

3. **Errores al sincronizar datos**: Revisa los logs de la aplicación para ver mensajes de error específicos:
```bash
docker-compose logs api
```
