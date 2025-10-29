#   Alfredo Barranco Ahued
#   ORM para la Pared Eólica usando Supabase REST API
#   Versión 3.0 - Sin necesidad de IPv4

from flask import Flask, request, abort, jsonify
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import os
from flask_cors import CORS
import pytz
from supabase import create_client, Client
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Load environment variables
load_dotenv()

BASE_URL = '/api/v1'
mexico_tz = pytz.timezone('America/Mexico_City')

# -----------------------------------------------------------------------
# CONFIGURACIÓN DE SUPABASE
# -----------------------------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")  # https://tmjwqdfibdjlszfklyxz.supabase.co
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Tu anon/service_role key

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ SUPABASE_URL y SUPABASE_KEY deben estar definidas")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
logger.info("✓ Cliente Supabase inicializado")

# -----------------------------------------------------------------------
# FUNCIONES DE ACTUALIZACIÓN
# -----------------------------------------------------------------------

def update_total_day(today, total_sum, sum_group1, sum_group2, sum_group3):
    """Actualiza o crea el registro del día"""
    try:
        # Buscar si existe
        result = supabase.table('total_day').select('*').eq('date', str(today)).execute()
        
        if result.data:
            # Actualizar existente
            current = result.data[0]
            supabase.table('total_day').update({
                'total': current['total'] + total_sum,
                'group1': current['group1'] + sum_group1,
                'group2': current['group2'] + sum_group2,
                'group3': current['group3'] + sum_group3
            }).eq('id', current['id']).execute()
        else:
            # Crear nuevo
            supabase.table('total_day').insert({
                'date': str(today),
                'total': total_sum,
                'group1': sum_group1,
                'group2': sum_group2,
                'group3': sum_group3
            }).execute()
    except Exception as e:
        logger.error(f"Error en update_total_day: {e}")

def update_total_month(month, total_sum):
    """Actualiza o crea el registro del mes"""
    try:
        if isinstance(month, str):
            month = datetime.strptime(month, '%Y-%m')
        
        month_start = month.replace(day=1).date()
        
        # Buscar si existe
        result = supabase.table('total_month').select('*').eq('date', str(month_start)).execute()
        
        if result.data:
            # Actualizar existente
            current = result.data[0]
            supabase.table('total_month').update({
                'total': current['total'] + total_sum
            }).eq('id', current['id']).execute()
        else:
            # Crear nuevo
            supabase.table('total_month').insert({
                'date': str(month_start),
                'total': total_sum
            }).execute()
    except Exception as e:
        logger.error(f"Error en update_total_month: {e}")

def update_total_all(total_sum):
    """Actualiza el total general"""
    try:
        # Buscar el registro (solo debe haber uno)
        result = supabase.table('total_all').select('*').execute()
        
        if result.data:
            # Actualizar existente
            current = result.data[0]
            supabase.table('total_all').update({
                'total': current['total'] + total_sum
            }).eq('id', current['id']).execute()
        else:
            # Crear nuevo
            supabase.table('total_all').insert({
                'total': total_sum
            }).execute()
    except Exception as e:
        logger.error(f"Error en update_total_all: {e}")

# -----------------------------------------------------------------------
# RUTAS
# -----------------------------------------------------------------------

@app.route('/')
def index():
    return "Welcome to my ORM app!"

# ---POST---------------------------------------------------------------

@app.route(BASE_URL + '/new', methods=['POST'])
def create():
    try:
        date = datetime.now(mexico_tz)
        date_time = date.strftime('%Y-%m-%d %H:%M:%S')
        today = date.date()
        month = date.strftime('%Y-%m')

        data = request.get_json()

        if not request.json or 'propeller1' not in request.json:
            abort(400)

        total_sum = sum([
            data['propeller1'],
            data['propeller2'],
            data['propeller3'],
            data['propeller4'],
            data['propeller5']
        ])

        if total_sum < 0.2:
            return jsonify({'message': 'Data not saved. Total sum is less than 0.2'})

        # Crear registros
        wall_data = {
            'date': date_time,
            'group': data['group'],
            'propeller1': data['propeller1'],
            'propeller2': data['propeller2'],
            'propeller3': data['propeller3'],
            'propeller4': data['propeller4'],
            'propeller5': data['propeller5']
        }

        supabase.table('wall_data').insert(wall_data).execute()
        supabase.table('temp_wall_data').insert(wall_data).execute()

        # Actualizar totales
        sum_group1 = data['propeller1'] + data['propeller2']
        sum_group2 = data['propeller3']
        sum_group3 = data['propeller4'] + data['propeller5']

        update_total_day(today, total_sum, sum_group1, sum_group2, sum_group3)
        update_total_month(month, total_sum)
        update_total_all(total_sum)

        return jsonify({**wall_data, 'message': 'Data saved successfully'})

    except Exception as e:
        logger.error(f"Error in create: {e}")
        return jsonify({'error': str(e)}), 500

# ---GET----------------------------------------------------------------

@app.route(BASE_URL + '/getWeek', methods=['GET'])
def get_week():
    try:
        today = datetime.now(mexico_tz).date()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
        
        result = supabase.table('total_day')\
            .select('*')\
            .gte('date', str(week_start))\
            .lte('date', str(week_end))\
            .execute()
        
        week_totals = {}
        total_week = 0
        
        for day in result.data:
            date_obj = datetime.strptime(day['date'], '%Y-%m-%d').date()
            date_formatted = date_obj.strftime('%A, %Y-%m-%d')
            week_totals[date_formatted] = (day['total'] ** 2 / 216 * 1000)
            total_week += day['total']
        
        return jsonify({'week_totals': week_totals, 'total_week': total_week})
    except Exception as e:
        logger.error(f"Error in getWeek: {e}")
        return jsonify({'error': str(e)}), 500
    
@app.route(BASE_URL + '/readTempLatest/<number>', methods=['GET'])
def readTempLatest(number):
    try:
        result = supabase.table('temp_wall_data')\
            .select('*')\
            .eq('group', number)\
            .order('id', desc=True)\
            .limit(1)\
            .execute()
        
        if not result.data:
            return jsonify({'message': 'No data found'}), 404
        
        return jsonify(result.data[0])
    except Exception as e:
        logger.error(f"Error in readTempLatest: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/readLatest', methods=['GET'])
def readLatest():
    try:
        result = supabase.table('wall_data')\
            .select('*')\
            .order('id', desc=True)\
            .limit(1)\
            .execute()
        
        if not result.data:
            return jsonify({'message': 'No data found'}), 404
        
        return jsonify(result.data[0])
    except Exception as e:
        logger.error(f"Error in readLatest: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/readAll', methods=['GET'])
def readAll():
    try:
        result = supabase.table('wall_data').select('*').execute()
        return jsonify(result.data)
    except Exception as e:
        logger.error(f"Error in readAll: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getAllHours', methods=['GET'])
def get_all_hours():
    try:
        date_str = request.args.get('date')
        if not date_str:
            date_obj = datetime.now(mexico_tz).date()
        else:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()

        # Obtener datos del día
        result = supabase.table('wall_data')\
            .select('*')\
            .gte('date', f'{date_obj} 00:00:00')\
            .lt('date', f'{date_obj + timedelta(days=1)} 00:00:00')\
            .execute()

        hourly_totals = {hour: 0 for hour in range(24)}

        for data in result.data:
            # Manejar ambos formatos de fecha
            date_str = data['date']
            if 'T' in date_str:
                # Formato ISO 8601 de Supabase
                hour = datetime.fromisoformat(date_str.replace('Z', '+00:00')).hour
            else:
                # Formato tradicional
                hour = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').hour
            
            total = sum([
                data['propeller1'] ** 2 / 216 * 1000,
                data['propeller2'] ** 2 / 216 * 1000,
                data['propeller3'] ** 2 / 216 * 1000,
                data['propeller4'] ** 2 / 216 * 1000,
                data['propeller5'] ** 2 / 216 * 1000
            ])
            hourly_totals[hour] += total

        return jsonify(hourly_totals)
    except Exception as e:
        logger.error(f"Error in getAllHours: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getCurrentDay', methods=['GET'])
def get_current_day():
    try:
        today = datetime.now(mexico_tz).date()
        result = supabase.table('total_day')\
            .select('*')\
            .eq('date', str(today))\
            .execute()

        if not result.data:
            return jsonify({'total': 0})
        
        return jsonify(result.data[0])
    except Exception as e:
        logger.error(f"Error in getCurrentDay: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/readAllDays', methods=['GET'])
def readAllDays():
    try:
        result = supabase.table('total_day').select('*').execute()
        return jsonify(result.data)
    except Exception as e:
        logger.error(f"Error in readAllDays: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getCurrentMonth', methods=['GET'])
def get_current_month():
    try:
        today = datetime.now(mexico_tz).date()
        month_start = today.replace(day=1)
        
        result = supabase.table('total_month')\
            .select('*')\
            .eq('date', str(month_start))\
            .execute()

        if not result.data:
            return jsonify({'total': 0})
        
        return jsonify(result.data[0])
    except Exception as e:
        logger.error(f"Error in getCurrentMonth: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/readAllMonths', methods=['GET'])
def readAllMonths():
    try:
        result = supabase.table('total_month').select('*').execute()
        
        month_totals = {month: 0 for month in range(1, 13)}
        
        for data in result.data:
            month = datetime.strptime(data['date'], '%Y-%m-%d').month
            month_totals[month] += data['total']

        return jsonify(month_totals)
    except Exception as e:
        logger.error(f"Error in readAllMonths: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getTotal', methods=['GET'])
def get_total():
    try:
        result = supabase.table('total_all').select('*').execute()

        if not result.data:
            return jsonify({'total': 0})
        
        return jsonify(result.data[0])
    except Exception as e:
        logger.error(f"Error in getTotal: {e}")
        return jsonify({'error': str(e)}), 500

# ---DELETE-------------------------------------------------------------

@app.route(BASE_URL + '/resetAll', methods=['DELETE'])
def resetAll():
    try:
        supabase.table('wall_data').delete().neq('id', 0).execute()
        supabase.table('total_day').delete().neq('id', 0).execute()
        supabase.table('total_month').delete().neq('id', 0).execute()
        supabase.table('total_all').delete().neq('id', 0).execute()
        return jsonify({'message': 'All data has been deleted'})
    except Exception as e:
        logger.error(f"Error in resetAll: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/resetTempWallData', methods=['DELETE'])
def resetTempWallData():
    try:
        supabase.table('temp_wall_data').delete().neq('id', 0).execute()
        return jsonify({'message': 'All temp data has been deleted'})
    except Exception as e:
        logger.error(f"Error in resetTempWallData: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("✓ Aplicación iniciada con Supabase REST API")
    app.run(debug=False)