import os
import sys
import yaml
import json
import argparse
from typing import Dict, Any, Tuple
from datetime import datetime, timezone
import requests
import re
import hashlib
import pathlib
import boto3
import time
import subprocess
import random
from tqdm.notebook import tqdm
import glob
from concurrent.futures import ThreadPoolExecutor

fecha_iso = datetime.now(timezone.utc).isoformat() + "Z"

# Directorio para almacenar el caché
CACHE_DIR = pathlib.Path(os.path.expanduser("~/.ai_github_analyzer_cache"))

def get_content_hash(content: Dict[Any, Any]) -> str:
    """Genera un hash único para el contenido del archivo YAML."""
    content_str = yaml.dump(content, sort_keys=True)
    return hashlib.md5(content_str.encode('utf-8')).hexdigest()

def get_from_cache(yaml_content: Dict[Any, Any]) -> Tuple[bool, str, str]:
    """Intenta recuperar un resultado previamente almacenado del caché."""
    if not CACHE_DIR.exists():
        # Si el directorio de caché no existe, lo creamos
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return None
    
    content_hash = get_content_hash(yaml_content)
    cache_file = CACHE_DIR / f"{content_hash}.json"
    
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                cached_result = json.load(f)
                return (
                    cached_result.get("es_puro", False),
                    json.dumps(cached_result.get("result", {}), indent=2),
                    cached_result.get("raw_response", "")
                )
        except Exception:
            # Si hay algún error al leer el caché, ignorarlo y continuar
            pass
    
    return None

def save_to_cache(yaml_content: Dict[Any, Any], is_pure: bool, json_result: str, raw_response: str) -> None:
    """Guarda el resultado del análisis en el caché."""
    try:
        # Crear directorio de caché si no existe
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        content_hash = get_content_hash(yaml_content)
        cache_file = CACHE_DIR / f"{content_hash}.json"
        
        # Guardamos tanto el resultado como la respuesta completa
        cache_data = {
            "es_puro": is_pure,
            "result": json.loads(json_result) if isinstance(json_result, str) else json_result,
            "raw_response": raw_response,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        print(f"⚠️ Advertencia: No se pudo guardar en caché: {e}")

def parse_yaml_file(file_path: str) -> Dict[Any, Any]:
    """
    Intenta parsear un archivo YAML.
    
    En lugar de detener la ejecución con sys.exit, ahora lanza excepciones
    que pueden ser capturadas por las funciones que la llaman.
    """
    try:
        # Leer el contenido del archivo
        with open(file_path, 'r') as file:
            content = file.read()
            
        # Verificar si el archivo está vacío
        if not content.strip():
            print(f"⚠️ Advertencia: El archivo YAML {file_path} está vacío")
            raise ValueError("El archivo YAML está vacío")
        
        # Corregir problemas comunes en el YAML
        original_content = content
        
        # 1. Corregir 'true:' que debería ser 'on:'
        if re.search(r'^\s*true\s*:', content, re.MULTILINE):
            content = re.sub(r'^\s*true\s*:', 'on:', content, flags=re.MULTILINE)
            print(f"🔧 Corregido: Reemplazado 'true:' por 'on:' en {file_path}")
        
        # Intentar parsear el YAML con las correcciones
        try:
            yaml_content = yaml.safe_load(content)
            
            # Si el contenido es None pero el archivo no está vacío, es probable que haya otro problema
            if yaml_content is None and content.strip():
                print(f"⚠️ Advertencia: El contenido YAML de {file_path} no pudo ser interpretado")
                raise yaml.YAMLError("El contenido YAML no pudo ser interpretado aunque no está vacío")
                
            # Si hemos llegado hasta aquí, la corrección funcionó
            if content != original_content:
                print(f"✅ Correcciones aplicadas a {file_path}")
                
                # Opcionalmente, guardar el archivo corregido
                with open(file_path, 'w') as file:
                    file.write(content)
                    print(f"✅ Guardado archivo YAML corregido: {file_path}")
                
            return yaml_content
        except yaml.YAMLError as e:
            # Si aún hay error después de las correcciones, lanzar la excepción
            print(f"❌ Error de sintaxis YAML en {file_path} (incluso después de correcciones): {e}")
            raise
    except yaml.YAMLError as e:
        print(f"Error al parsear el archivo YAML: {e}")
        raise  # Lanzar la excepción para ser capturada por el código que llama
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        raise  # Lanzar la excepción para ser capturada por el código que llama

def analyze_with_ai(yaml_content: Dict[Any, Any], file_path: str, reference_content: Dict[Any, Any] = None) -> Tuple[bool, str, str]:
    # Primero intentamos recuperar del caché
    cached_result = get_from_cache(yaml_content)
    if cached_result:
        print("🔄 Usando resultado en caché")
        return cached_result
    
    # Verificar si el contenido YAML es None
    if yaml_content is None:
        print(f"⚠️ Advertencia: El contenido YAML de {file_path} es None. No se puede analizar.")
        repo_name = extract_repo_name(file_path)
        pipeline_name = "No definido"
        default_response = {
            "es_puro": False,
            "type_pipeline": "impuro",
            "service": "unknown",
            "nombre_pipeline": pipeline_name,
            "repositorio": repo_name,
            "fecha_analisis": fecha_iso,
            "violaciones": [
                {
                    "job": "unknown",
                    "regla": "0",
                    "descripcion": "No se pudo analizar el archivo debido a que el contenido YAML es nulo o inválido."
                }
            ]
        }
        return False, json.dumps(default_response, indent=2), "Contenido YAML inválido"
    
    yaml_str = yaml.dump(yaml_content, default_flow_style=False)
    pipeline_name = yaml_content.get("name", "No definido")
    repo_name = extract_repo_name(file_path)
    prompt = ""

    if reference_content:
        reference_str = yaml.dump(reference_content, default_flow_style=False)
        prompt = f"""
Valida si este archivo YAML de GitHub Actions representa un "pipeline puro" según las siguientes reglas de la organización:

1. Todos los valores en los campos `uses` dentro de `jobs` deben comenzar con:  
`Cencosud-Cencommerce/actions/.github`.

2. No se permite que un `job` tenga pasos definidos (clave `steps`), ya que un pipeline puro debe delegar toda su lógica a workflows reutilizables.

3. Todos los uses para que se consideren "puros" deben contener los siguientes nombres: `airflow.yml` `build-container.yml` `docker-mirror.yml` `tf-flow-legacy.yml` `tf-flow-nr.yml` `tf-flow.yml` `deploy-to-eb-legacy-java.yml` `deploy-to-eb-legacy-node.yml` `deploy-to-ecs.yml` `deploy-to-gh-pages.yml` `deploy-to-ios.yml` `deploy-to-k8s-uat.yml` `deploy-to-k8s.yml` `deploy-to-lambda-sam.yml` `deploy-to-lambda-zip.yml` `deploy-to-lambda.yml` `deploy-to-s3.yml`, y ademas en su definición al final deben tener `@main`. 
En el caso de que tengas un action que tenga un combinación entre la lista proporcionada y otro nombre.

4. En el caso de que tengas un action que tenga un combinación entre la lista proporcionada y otro nombre, debes crear un nuevo item en el json de respuesta con el nombre de del tipo de tecnologia que se esta usando, a continuación un ejemplo:

- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/airflow.yml` es servicio que esta usando es: airflow
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-k8s.yml` es servicio que esta usando es: kubernetes
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-s3.yml` es servicio que esta usando es: s3
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-eb-legacy-java.yml` es servicio que esta usando es: java
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-eb-legacy-node.yml` es servicio que esta usando es: node
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-ecs.yml` es servicio que esta usando es: ecs
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-gh-pages.yml` es servicio que esta usando es: gh-pages
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-ios.yml` es servicio que esta usando es: ios
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-k8s-uat.yml` es servicio que esta usando es: k8s-uat
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-lambda-sam.yml` es servicio que esta usando es: lambda-sam
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-lambda-zip.yml` es servicio que esta usando es: lambda
- El pipeline que utiliza `Cencosud-Cencommerce/actions/.github/workflows/deploy-to-lambda.yml` es servicio que esta usando es: lambda



La respuesta que debes devolver debe ser en formato JSON con la siguiente estructura:
```json
{{
  "es_puro": true | false,
  "service": "kubernetes | airflow | s3 ",
  "type_pipeline": "pure",
  "nombre_pipeline": "{pipeline_name}",
  "repositorio": "{repo_name}",
  "fecha_analisis": "{fecha_iso}",
  "violaciones": [
    {{
      "job": "nombre_del_job",
      "regla": "1 | 2 | 3",
      "descripcion": "Texto que explica el motivo de la violación"
    }}
  ]
}}
```

Este es un ejemplo de pipeline puro de referencia:
```yaml
{reference_str}
```

Este es el contenido YAML a validar:
```yaml
{yaml_str}
```

El archivo analizado pertenece al repositorio: {repo_name}
"""
    try:
        # URL de la API de Ollama (por defecto en localhost:11434)
        ollama_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")
        
        # Modelo a utilizar (por defecto deepseek-r1:8b)
        ollama_model = os.environ.get("OLLAMA_MODEL", "deepseek-r1:14b")
        print(f"🤖 Usando modelo: {ollama_model}")
        
        # Preparar los datos para la llamada a la API de Ollama
        data = {
            "model": ollama_model,
            "messages": [
                {"role": "system", "content": "Eres un experto en GitHub Actions. Analiza YAMLs para verificar si son 'pipelines puros' según reglas organizacionales."},
                {"role": "user", "content": prompt}
            ],
            "stream": False
        }
        
        # Realizar la llamada a la API de Ollama
        response = requests.post(f"{ollama_url}/api/chat", json=data)
        response.raise_for_status()  # Lanza una excepción si hay un error HTTP
        
        response_json = response.json()

        ai_response = response_json.get("message", {}).get("content", "").strip()
        print(f"🤖 Respuesta de la API de Ollama: {ai_response}")
        
        # Verificar si la respuesta está vacía
        if not ai_response:
            print(f"⚠️ Advertencia: El modelo devolvió una respuesta vacía. Usando respuesta predeterminada.")
            # Crear una respuesta predeterminada para evitar errores
            repo_name = extract_repo_name(file_path)
            pipeline_name = yaml_content.get("name", "No definido")
            default_response = {
                "es_puro": False,
                "type_pipeline": "impuro",
                "service": "unknown",
                "nombre_pipeline": pipeline_name,
                "repositorio": repo_name,
                "fecha_analisis": fecha_iso,
                "violaciones": [
                    {
                        "job": "unknown",
                        "regla": "0",
                        "descripcion": "No se pudo analizar el archivo debido a un error en la respuesta del modelo."
                    }
                ]
            }
            return False, json.dumps(default_response, indent=2), "Respuesta del modelo vacía"

        try:
            result = json.loads(ai_response)
        except json.JSONDecodeError:
            json_match = re.search(r'```json\n(.*?)\n```|({.*})', ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1) or json_match.group(2)
                result = json.loads(json_str)
            else:
                return False, "No se pudo analizar la respuesta de la IA", ai_response

        # Asegurarnos de que el nombre del repositorio esté presente
        if "repositorio" not in result or not result["repositorio"]:
            result["repositorio"] = repo_name

        # Guardamos el resultado en caché para futuras consultas
        is_pure = result.get("es_puro", False)
        json_result = json.dumps(result, indent=2)
        save_to_cache(yaml_content, is_pure, result, ai_response)
        
        return is_pure, json_result, ai_response

    except Exception as e:
        print(f"❌ Error al comunicarse con la API de Ollama: {e}")
        return False, f"Error en la API: {str(e)}", ""

def get_dynamodb_table():
    """Obtiene la tabla DynamoDB para guardar los resultados."""
    try:
        # Usar variables de entorno para la configuración
        is_local = os.environ.get("IS_LOCAL", "true").lower() == "true"
        
        # Si se especificó un endpoint personalizado, usamos modo local
        custom_endpoint = os.environ.get("DYNAMODB_ENDPOINT", None)
        if custom_endpoint and ("localhost" in custom_endpoint or "localstack" in custom_endpoint):
            is_local = True
        
        # Siempre especificar región, incluso para AWS real
        region = "us-east-1"  # Región por defecto
        
        if is_local:
            print(f"🔄 Usando configuración local para DynamoDB (LocalStack)")
            # Configuración para LocalStack
            endpoint_url = custom_endpoint or 'http://localhost:4566'
            dynamodb = boto3.resource(
                'dynamodb',
                endpoint_url=endpoint_url,
                region_name=region,
                aws_access_key_id='test',
                aws_secret_access_key='test'
            )
        else:
            print(f"🔄 Usando configuración AWS real para DynamoDB")
            # Configuración para AWS real con región especificada
            dynamodb = boto3.resource('dynamodb', region_name=region)
        
        table_name = os.environ.get("DYNAMODB_TABLE", "github-actions-analysis")
        table = dynamodb.Table(table_name)
        
        return table
    except Exception as e:
        print(f"❌ Error al conectar con DynamoDB: {e}")
        return None

def initialize_dynamodb():
    """Verifica que la tabla DynamoDB existe y la crea si no existe."""
    try:
        # Usar variables de entorno para la configuración
        is_local = os.environ.get("IS_LOCAL", "true").lower() == "true"
        
        # Si se especificó un endpoint personalizado, usamos modo local
        custom_endpoint = os.environ.get("DYNAMODB_ENDPOINT", None)
        if custom_endpoint and ("localhost" in custom_endpoint or "localstack" in custom_endpoint):
            is_local = True
        
        # Siempre especificar región, incluso para AWS real
        region = "us-east-1"  # Región por defecto
        
        if is_local:
            print(f"🔄 Usando configuración local para DynamoDB (LocalStack)")
            # Configuración para LocalStack
            endpoint_url = custom_endpoint or 'http://localhost:4566'
            dynamodb = boto3.resource(
                'dynamodb',
                endpoint_url=endpoint_url,
                region_name=region,
                aws_access_key_id='test',
                aws_secret_access_key='test'
            )
            dynamodb_client = boto3.client(
                'dynamodb',
                endpoint_url=endpoint_url,
                region_name=region,
                aws_access_key_id='test',
                aws_secret_access_key='test'
            )
        else:
            print(f"🔄 Usando configuración AWS real para DynamoDB")
            # Configuración para AWS real con región especificada
            dynamodb = boto3.resource('dynamodb', region_name=region)
            dynamodb_client = boto3.client('dynamodb', region_name=region)
        
        table_name = os.environ.get("DYNAMODB_TABLE", "github-actions-analysis")
        
        # Verificar si la tabla existe
        existing_tables = dynamodb_client.list_tables()["TableNames"]
        if table_name in existing_tables:
            print(f"✅ Tabla DynamoDB '{table_name}' ya existe")
            return True
        
        # Crear tabla si no existe
        print(f"🔄 Creando tabla DynamoDB '{table_name}'...")
        
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'pipeline_name', 'KeyType': 'HASH'},  # Partition key
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}  # Sort key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pipeline_name', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'},
                {'AttributeName': 'type_pipeline', 'AttributeType': 'S'},
                {'AttributeName': 'service', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'TypePipelineIndex',
                    'KeySchema': [
                        {'AttributeName': 'type_pipeline', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                },
                {
                    'IndexName': 'ServiceIndex',
                    'KeySchema': [
                        {'AttributeName': 'service', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            Tags=[
                {'Key': 'Environment', 'Value': 'Development'},
                {'Key': 'Project', 'Value': 'GithubActionsAnalyzer'}
            ]
        )
        
        print(f"✅ Tabla DynamoDB '{table_name}' creada correctamente")
        return True
    except Exception as e:
        print(f"❌ Error al crear la tabla DynamoDB: {e}")
        return False

def extract_repo_name(file_path: str) -> str:
    """Extrae el nombre del repositorio a partir de la ruta del archivo.
    
    Asume que la estructura es workflows/<nombre_repo>/archivo.yml o 
    similar donde el penúltimo directorio es el nombre del repo.
    """
    try:
        # Normalizar path para manejo consistente
        normalized_path = os.path.normpath(file_path)
        
        # Dividir la ruta en componentes
        path_parts = normalized_path.split(os.sep)
        
        # Si hay "workflows" en la ruta, extraer el nombre después de workflows
        if "workflows" in path_parts:
            workflows_index = path_parts.index("workflows")
            if len(path_parts) > workflows_index + 1:
                return path_parts[workflows_index + 1]
        
        # Si no hay "workflows" pero hay al menos 2 componentes, usar el penúltimo
        if len(path_parts) >= 2:
            return path_parts[-2]
        
        # Si todo falla, usar el directorio actual
        return os.path.basename(os.path.dirname(os.path.abspath(file_path)))
    except Exception as e:
        print(f"⚠️ No se pudo extraer el nombre del repositorio: {e}")
        return "desconocido"

def save_to_dynamodb(analysis_result: Dict, pipeline_name: str, repo_name: str = None) -> bool:
    """Guarda los resultados del análisis en DynamoDB."""
    table = get_dynamodb_table()
    if not table:
        return False
    
    try:
        # Asegurar que exista el campo type_pipeline
        if "type_pipeline" not in analysis_result:
            analysis_result["type_pipeline"] = "pure" if analysis_result.get("es_puro", False) else "impuro"
        
        # Preparar item para guardar en DynamoDB
        item = {
            'pipeline_name': pipeline_name,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'service': analysis_result.get('service', ''),
            'es_puro': analysis_result.get('es_puro', False),
            'type_pipeline': analysis_result.get('type_pipeline', 'impuro'),
            'violaciones': analysis_result.get('violaciones', []),
            'fecha_analisis': analysis_result.get('fecha_analisis'),
            'nombre_pipeline': analysis_result.get('nombre_pipeline', pipeline_name),
            'repositorio': repo_name or analysis_result.get('repositorio') or "desconocido",
            # Configurar TTL: datos expirarán en 30 días
            'ttl': int(time.time()) + (30 * 24 * 60 * 60)
        }
        
        # Guardar en DynamoDB
        table.put_item(Item=item)
        print(f"✅ Datos guardados correctamente en DynamoDB para repositorio: {item['repositorio']}")
        return True
    except Exception as e:
        print(f"❌ Error al guardar en DynamoDB: {e}")
        return False

def send_to_sqs(json_result: str, pipeline_name: str, queue_url: str, repo_name: str = None) -> Dict:
    """Envía el resultado del análisis a una cola SQS."""
    try:
        # Configuración para LocalStack o AWS real
        is_local = os.environ.get("IS_LOCAL", "true").lower() == "true"
        
        # Verificar si la URL apunta a LocalStack
        is_localstack_url = "localhost" in queue_url or "localstack" in queue_url
        
        # Forzar modo local si la URL es de LocalStack
        if is_localstack_url:
            is_local = True
        
        # Siempre especificar región, incluso para AWS real
        region = "us-east-1"  # Región por defecto
        
        if is_local:
            print(f"🔄 Usando configuración local para SQS (LocalStack)")
            sqs_client = boto3.client(
                'sqs',
                endpoint_url='http://localhost:4566',
                region_name=region,
                aws_access_key_id='test',
                aws_secret_access_key='test'
            )
        else:
            print(f"🔄 Usando configuración AWS real para SQS")
            # Para AWS real, usar credenciales del perfil de AWS CLI
            sqs_client = boto3.client('sqs', region_name=region)
        
        # Parsear el resultado JSON
        analysis_result = json.loads(json_result)
        
        # Asegurar que exista el campo type_pipeline
        if "type_pipeline" not in analysis_result:
            analysis_result["type_pipeline"] = "pure" if analysis_result.get("es_puro", False) else "impuro"
        
        # Agregar o actualizar el nombre del repositorio
        if repo_name:
            analysis_result["repositorio"] = repo_name
        
        # Crear mensaje con metadatos adicionales
        message = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_name": pipeline_name,
            "repo_name": repo_name or "desconocido",
            "analysis_result": analysis_result
        }
        
        # Si estamos usando LocalStack pero la URL no lo refleja, corregir
        if is_local and not is_localstack_url:
            # Convertir a URL de LocalStack
            print(f"⚠️ Corrigiendo URL para LocalStack: {queue_url}")
            queue_name = queue_url.split("/")[-1]
            queue_url = f"http://localhost:4566/000000000000/{queue_name}"
            print(f"✅ URL corregida: {queue_url}")
        
        # Enviar mensaje a la cola SQS
        print(f"🔄 Enviando mensaje a: {queue_url}")
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        
        print(f"✅ Resultado enviado a la cola SQS. MessageId: {response.get('MessageId')}")
        return response
    
    except Exception as e:
        print(f"❌ Error al enviar mensaje a SQS: {e}")
        print(f"⚠️ URL de la cola utilizada: {queue_url}")
        return {"error": str(e)}

def print_results(input_file, is_pure, json_result, raw_response, yaml_content, reference_file=None, reference_content=None):
    if is_pure:
        print("✅ Este GitHub Action es PURO")
    else:
        print("❌ Este GitHub Action NO es PURO")

    # Extraer el nombre del repositorio de la ruta del archivo
    repo_name = extract_repo_name(input_file)
    print(f"📁 Repositorio: {repo_name}")

    print(f"\n📊 Resultado en formato JSON:")
    print(json_result)

    # Guardar en DynamoDB directamente si no hay SQS configurado
    analysis_result = json.loads(json_result)
    pipeline_name = yaml_content.get('name', 'No definido')
    
    # Enviar resultado a SQS si está configurado el URL de la cola
    queue_url = os.environ.get("SQS_QUEUE_URL")
    if queue_url:
        print(f"🔄 Enviando resultado a la cola SQS: {queue_url}")
        send_to_sqs(json_result, pipeline_name, queue_url, repo_name=repo_name)
    else:
        # Si no hay SQS configurado, guardar directamente en DynamoDB
        print(f"🔄 Guardando resultado directamente en DynamoDB...")
        save_to_dynamodb(analysis_result, pipeline_name, repo_name=repo_name)

def process_multiple_repos(base_dir='workflows', num_repos=5, random_selection=True):
    """
    Procesa múltiples repositorios utilizando el script ai_github_actions_analyzer.py
    
    Args:
        base_dir: Directorio base donde se encuentran los repositorios
        num_repos: Número de repositorios a procesar
        random_selection: Si es True, selecciona repos aleatoriamente, de lo contrario toma los primeros
    """
    # Listar todos los directorios (repositorios) en base_dir
    repos = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    print(f"Se encontraron {len(repos)} repositorios en total")
    
    # Limitar la cantidad de repositorios a procesar
    if num_repos > len(repos):
        print(f"Solo hay {len(repos)} repositorios disponibles, se procesarán todos")
        num_repos = len(repos)
    
    # Seleccionar repositorios (aleatorios o primeros N)
    if random_selection:
        selected_repos = random.sample(repos, num_repos)
    else:
        selected_repos = repos[:num_repos]
    
    print(f"Se procesarán {num_repos} repositorios:")
    for i, repo in enumerate(selected_repos):
        print(f"{i+1}. {repo}")
    
    # Errores encontrados durante el procesamiento
    errors = []
    
    # Procesar cada repositorio
    results = []
    for repo in tqdm(selected_repos, desc="Procesando repositorios"):
        repo_path = os.path.join(base_dir, repo)
        
        # Buscar archivos YAML en el repositorio
        yaml_files = []
        for root, dirs, files in os.walk(repo_path):
            for file in files:
                if file.endswith(('.yml', '.yaml')):
                    yaml_files.append(os.path.join(root, file))
        
        if not yaml_files:
            print(f"No se encontraron archivos YAML en {repo}")
            continue
        
        # Procesar el primer archivo YAML encontrado (o todos si prefieres)
        for yaml_file in yaml_files:
            print(f"\nAnalizando {yaml_file}")
            try:
                # Ejecutar el script para analizar este archivo
                cmd = f"python ai_github_actions_analyzer.py {yaml_file}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                # Verificar si se ejecutó correctamente
                if result.returncode == 0:
                    print(f"✅ Análisis completado para {yaml_file}")
                    # Extraer resultado del output si es necesario
                    output_lines = result.stdout.split('\n')
                    for line in output_lines:
                        if "es_puro" in line:
                            print(f"Resultado: {line}")
                            break
                else:
                    print(f"❌ Error al analizar {yaml_file}: {result.stderr}")
                
                results.append({
                    'repo': repo,
                    'file': yaml_file,
                    'success': result.returncode == 0,
                    'output': result.stdout,
                    'error': result.stderr
                })
                
            except Exception as e:
                print(f"❌ Error inesperado al procesar {yaml_file}: {e}")
                results.append({
                    'repo': repo,
                    'file': yaml_file,
                    'success': False,
                    'error': str(e)
                })
                errors.append({
                    'repo': repo,
                    'file': yaml_file,
                    'error_type': 'UNHANDLED_ERROR',
                    'error_msg': str(e)
                })
    
    # Resumen
    success_count = sum(1 for r in results if r['success'])
    print(f"\nResumen del procesamiento:")
    print(f"Total de archivos procesados: {len(results)}")
    print(f"Exitosos: {success_count}")
    print(f"Fallidos: {len(results) - success_count}")
    
    # Guardar errores en un archivo si es necesario
    if errors:
        errors_file = f"yaml_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(errors_file, 'w') as f:
            json.dump({
                'total_errors': len(errors),
                'errors': errors
            }, f, indent=2)
        print(f"Errores guardados en: {errors_file}")
    
    return results

def process_workflow_batch(file_paths, max_workers=4):
    """Procesa un lote de archivos de workflow en paralelo."""
    results = []
    
    def process_single_file(file_path):
        try:
            yaml_content = parse_yaml_file(file_path)
            is_pure, json_result, _ = analyze_with_ai(yaml_content, file_path)
            result_dict = json.loads(json_result)
            repo_name = extract_repo_name(file_path)
            save_to_dynamodb(result_dict, result_dict.get('nombre_pipeline', 'No definido'), repo_name)
            return result_dict
        except Exception as e:
            print(f"Error al procesar {file_path}: {e}")
            return None
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_results = list(executor.map(process_single_file, file_paths))
    
    # Filtrar resultados None (errores)
    results = [r for r in future_results if r is not None]
    return results

def find_yaml_files(base_dir='workflows', patterns=['*.yml', '*.yaml']):
    """Encuentra todos los archivos YAML en el directorio."""
    all_files = []
    for pattern in patterns:
        search_pattern = os.path.join(base_dir, '**', pattern)
        all_files.extend(glob.glob(search_pattern, recursive=True))
    return all_files

def process_repositories_batch(workflows_dir, num_repos=0, random_selection=False):
    """Procesa múltiples repositorios en lote."""
    import random
    
    # Listar todos los directorios (repositorios) en workflows_dir
    repos = [d for d in os.listdir(workflows_dir) if os.path.isdir(os.path.join(workflows_dir, d))]
    print(f"Se encontraron {len(repos)} repositorios en total")
    
    # Limitar la cantidad de repositorios a procesar
    if num_repos <= 0 or num_repos > len(repos):
        num_repos = len(repos)
        print(f"Se procesarán todos los {num_repos} repositorios")
    else:
        print(f"Se procesarán {num_repos} repositorios")
    
    # Seleccionar repositorios (aleatorios o primeros N)
    if random_selection:
        selected_repos = random.sample(repos, num_repos)
    else:
        selected_repos = repos[:num_repos]
    
    # Mostrar repositorios seleccionados
    for i, repo in enumerate(selected_repos):
        print(f"{i+1}. {repo}")
    
    # Procesar cada repositorio
    results = []
    errors = []
    
    for repo in selected_repos:
        repo_path = os.path.join(workflows_dir, repo)
        
        # Buscar archivos YAML en el repositorio
        yaml_files = []
        for root, dirs, files in os.walk(repo_path):
            for file in files:
                if file.endswith(('.yml', '.yaml')):
                    yaml_files.append(os.path.join(root, file))
        
        if not yaml_files:
            print(f"No se encontraron archivos YAML en {repo}")
            continue
        
        # Procesar cada archivo YAML
        for yaml_file in yaml_files:
            print(f"\nAnalizando {yaml_file}")
            try:
                # Intentar parsear el YAML - ahora maneja excepciones adecuadamente
                try:
                    yaml_content = parse_yaml_file(yaml_file)
                except (yaml.YAMLError, yaml.MarkedYAMLError, ValueError) as e:
                    print(f"❌ Error YAML en {yaml_file}: {e}")
                    errors.append({
                        'repo': repo,
                        'file': yaml_file,
                        'error_type': 'YAML_PARSE_ERROR',
                        'error_msg': str(e)
                    })
                    continue
                except Exception as e:
                    print(f"❌ Error al leer archivo {yaml_file}: {e}")
                    errors.append({
                        'repo': repo,
                        'file': yaml_file,
                        'error_type': 'FILE_READ_ERROR',
                        'error_msg': str(e)
                    })
                    continue
                
                # Si el YAML es válido, continuar con el análisis
                try:
                    is_pure, json_result, raw_response = analyze_with_ai(yaml_content, yaml_file)
                    print_results(yaml_file, is_pure, json_result, raw_response, yaml_content)
                    
                    results.append({
                        'repo': repo,
                        'file': yaml_file,
                        'is_pure': is_pure,
                        'result': json.loads(json_result) if isinstance(json_result, str) else json_result
                    })
                except Exception as e:
                    print(f"❌ Error durante el análisis de {yaml_file}: {e}")
                    errors.append({
                        'repo': repo,
                        'file': yaml_file,
                        'error_type': 'ANALYSIS_ERROR',
                        'error_msg': str(e)
                    })
                
            except Exception as e:
                print(f"❌ Error no manejado para {yaml_file}: {e}")
                errors.append({
                    'repo': repo,
                    'file': yaml_file,
                    'error_type': 'UNHANDLED_ERROR',
                    'error_msg': str(e)
                })
    
    # Guardar errores en un archivo
    if errors:
        errors_file = f"yaml_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(errors_file, 'w') as f:
            json.dump({
                'total_errors': len(errors),
                'errors': errors
            }, f, indent=2)
        print(f"Errores guardados en: {errors_file}")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Analizador de GitHub Actions para pipelines puros")
    parser.add_argument("input_file", help="Archivo YAML de GitHub Actions a analizar")
    parser.add_argument("-r", "--reference", help="Archivo YAML de referencia para pipeline puro (opcional)")
    parser.add_argument("--no-cache", action="store_true", help="Desactiva el uso de caché")
    parser.add_argument("--model", help="Modelo de Ollama a utilizar (por defecto: llama3)")
    parser.add_argument("--url", help="URL de la API de Ollama (por defecto: http://localhost:11434)")
    parser.add_argument("--queue", help="URL de la cola SQS para enviar los resultados")
    parser.add_argument("--init-db", action="store_true", help="Inicializar la tabla DynamoDB")
    parser.add_argument("--dynamodb-endpoint", help="Endpoint de DynamoDB (para LocalStack)")
    parser.add_argument("--dynamodb-table", help="Nombre de la tabla DynamoDB")
    parser.add_argument("--local", action="store_true", help="Usar configuración local (LocalStack)")
    parser.add_argument("--batch", type=int, help="Número de repositorios a procesar (0 para todos)")
    parser.add_argument("--random", action="store_true", help="Seleccionar repositorios aleatoriamente")
    parser.add_argument("--workflows-dir", default="workflows", help="Directorio donde se encuentran los workflows")
    parser.add_argument("--continue-on-error", action="store_true", help="Continuar procesando aún con errores YAML")
    args = parser.parse_args()
    
    # Establecer variables de entorno para Ollama si se proporcionan
    if args.model:
        os.environ["OLLAMA_MODEL"] = args.model
    if args.url:
        os.environ["OLLAMA_URL"] = args.url
    if args.queue:
        os.environ["SQS_QUEUE_URL"] = args.queue
        
    # Establecer variables de entorno para servicios AWS
    os.environ["IS_LOCAL"] = "true" if args.local else "false"
    print(f"🔧 Modo local: {'Activado' if args.local else 'Desactivado'}")
    
    if args.dynamodb_endpoint:
        os.environ["DYNAMODB_ENDPOINT"] = args.dynamodb_endpoint
    if args.dynamodb_table:
        os.environ["DYNAMODB_TABLE"] = args.dynamodb_table
    
    # Inicializar DynamoDB si se solicita
    if args.init_db:
        if initialize_dynamodb():
            print("✅ Tabla DynamoDB inicializada correctamente")
        else:
            print("❌ Error al inicializar la tabla DynamoDB")
            sys.exit(1)
    
    # Dentro de main(), añade este código para procesar en lote si se especifica --batch
    if args.batch is not None:
        # Modo procesamiento por lotes
        results = process_repositories_batch(
            workflows_dir=args.workflows_dir,
            num_repos=args.batch,
            random_selection=args.random
        )
        
        # Mostrar resumen
        pure_count = sum(1 for r in results if r.get('is_pure', False))
        impure_count = len(results) - pure_count
        
        print("\nResumen del procesamiento en lote:")
        print(f"Total de archivos procesados: {len(results)}")
        
        # Calcular porcentajes solo si hay resultados
        if len(results) > 0:
            pure_percentage = pure_count/len(results)*100
            impure_percentage = impure_count/len(results)*100
        else:
            pure_percentage = 0
            impure_percentage = 0
            
        print(f"Pipelines puros: {pure_count} ({pure_percentage:.2f}%)")
        print(f"Pipelines impuros: {impure_count} ({impure_percentage:.2f}%)")
        
        # Opcionalmente, guardar resultados en un archivo JSON
        output_file = f"analysis_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Resultados guardados en: {output_file}")
    else:
        # Modo análisis de un solo archivo
    input_file = args.input_file
    reference_file = args.reference

    if not os.path.exists(input_file):
        print(f"❌ El archivo de entrada {input_file} no existe.")
        sys.exit(1)

    if reference_file and not os.path.exists(reference_file):
        print(f"❌ El archivo de referencia {reference_file} no existe.")
        sys.exit(1)

        try:
    yaml_content = parse_yaml_file(input_file)
    reference_content = parse_yaml_file(reference_file) if reference_file else None

    # Si se solicita no usar caché, eliminamos el archivo de caché correspondiente
    if args.no_cache:
        content_hash = get_content_hash(yaml_content)
        cache_file = CACHE_DIR / f"{content_hash}.json"
        if cache_file.exists():
            cache_file.unlink()
            print("🗑️ Ignorando caché para esta ejecución")

            is_pure, json_result, raw_response = analyze_with_ai(yaml_content, input_file, reference_content)
    print_results(input_file, is_pure, json_result, raw_response, yaml_content, reference_file, reference_content)
        except Exception as e:
            print(f"❌ Error al procesar el archivo {input_file}: {e}")
            if not args.continue_on_error:
                sys.exit(1)
            # Si se especifica continuar con errores, simplemente mostrar el error y salir con código 0
            print(f"Continuando debido a --continue-on-error")
            
            # Guardar el error en un archivo JSON para referencia
            error_info = {
                'file': input_file,
                'error_type': type(e).__name__,
                'error_msg': str(e)
            }
            error_file = f"error_{os.path.basename(input_file)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(error_file, 'w') as f:
                json.dump(error_info, f, indent=2)
            print(f"Detalles del error guardados en: {error_file}")

if __name__ == "__main__":
    main()
