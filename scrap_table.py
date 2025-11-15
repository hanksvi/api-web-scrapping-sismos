import requests
import boto3
import uuid
from datetime import datetime

def lambda_handler(event, context):
    # URL de la API del IGP
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"

    try:
        # Realizar la solicitud HTTP a la API
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json'
        })
        
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': 'Error al acceder a la API del IGP'
            }

        # Obtener los datos JSON
        sismos_data = response.json()
        
        if not sismos_data:
            return {
                'statusCode': 404,
                'body': 'No se encontraron datos de sismos'
            }

        # Procesar los datos para que coincidan con los headers de la tabla
        rows = []
        for sismo in sismos_data:
            # Formatear fecha y hora local
            fecha_local = datetime.fromisoformat(sismo['fecha_local'].replace('Z', '+00:00'))
            hora_local = datetime.fromisoformat(sismo['hora_local'].replace('Z', '+00:00'))
            
            fecha_hora_str = f"{fecha_local.strftime('%d/%m/%Y')} {hora_local.strftime('%H:%M:%S')}"
            
            row = {
                'Reporte sísmico': f"IGP/CENSIS/RS {sismo['codigo']}",
                'Referencia': sismo['referencia'],
                'Fecha y hora (Local)': fecha_hora_str,
                'Magnitud': str(sismo['magnitud']),
                'Descargas': sismo.get('reporte_acelerometrico_pdf', 'N/A')
            }
            rows.append(row)

        # Guardar los datos en DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TablaSismos')

        # Eliminar todos los elementos de la tabla antes de agregar los nuevos
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'id': each['id']
                    }
                )

        # Insertar los nuevos datos
        for i, row in enumerate(rows, 1):
            row['#'] = i
            row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
            table.put_item(Item=row)

        # Retornar el resultado como JSON
        return {
            'statusCode': 200,
            'body': rows
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }