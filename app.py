#   Alfredo Barranco Ahued
#   5 de octubre de 2024
#   ORM para la base de datos de la Pared Eólica para ASE II
#   Versión 2.1 - Usando create_engine

from flask import Flask, request, abort, jsonify
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import os
from flask_cors import CORS
import pytz
from sqlalchemy import create_engine, Column, Integer, Float, DateTime, Date, cast, func
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from contextlib import contextmanager
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
# CONFIGURACIÓN DE BASE DE DATOS CON create_engine
# -----------------------------------------------------------------------

# Obtener credenciales
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "db.tmjwqdfibdjlszfklyxz.supabase.co")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

if not DB_PASSWORD:
    raise ValueError("❌ DB_PASSWORD no está definida en las variables de entorno")

# Construir la URI de conexión
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Configurar el engine según el entorno
if os.getenv("VERCEL") == "1":
    # Para Vercel (serverless) - sin pool de conexiones
    engine = create_engine(
        DATABASE_URI,
        poolclass=NullPool,
        connect_args={
            "sslmode": "require",
            "connect_timeout": 10,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
        echo=False
    )
    logger.info("✓ Engine configurado para Vercel (NullPool)")
else:
    # Para desarrollo local - con pool de conexiones
    engine = create_engine(
        DATABASE_URI,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=5,
        max_overflow=10,
        connect_args={
            "sslmode": "require",
            "connect_timeout": 10,
        },
        echo=False
    )
    logger.info("✓ Engine configurado para desarrollo local")

# Crear session factory
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

# Context manager para manejar sesiones de manera segura
@contextmanager
def get_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Error en sesión de base de datos: {e}")
        raise
    finally:
        session.close()

# -----------------------------------------------------------------------
# MODELOS
# -----------------------------------------------------------------------

class TempWallData(Base):
    __tablename__ = 'temp_wall_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False)
    group = Column(Integer, nullable=False)
    propeller1 = Column(Float, nullable=False)
    propeller2 = Column(Float, nullable=False)
    propeller3 = Column(Float, nullable=False)
    propeller4 = Column(Float, nullable=False)
    propeller5 = Column(Float, nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'date': self.date.strftime('%Y-%m-%d %H:%M:%S'),
            'group': self.group,
            'propeller1': self.propeller1,
            'propeller2': self.propeller2,
            'propeller3': self.propeller3,
            'propeller4': self.propeller4,
            'propeller5': self.propeller5,
        }

class WallData(Base):
    __tablename__ = 'wall_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False)
    group = Column(Integer, nullable=False)
    propeller1 = Column(Float, nullable=False)
    propeller2 = Column(Float, nullable=False)
    propeller3 = Column(Float, nullable=False)
    propeller4 = Column(Float, nullable=False)
    propeller5 = Column(Float, nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'date': self.date.strftime('%Y-%m-%d %H:%M:%S'),
            'group': self.group,
            'propeller1': self.propeller1,
            'propeller2': self.propeller2,
            'propeller3': self.propeller3,
            'propeller4': self.propeller4,
            'propeller5': self.propeller5,
        }

class TotalDay(Base):
    __tablename__ = 'total_day'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    total = Column(Float, nullable=False)
    group1 = Column(Float, nullable=False)
    group2 = Column(Float, nullable=False)
    group3 = Column(Float, nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'date': self.date.strftime('%Y-%m-%d'),
            'total': self.total,
            'group1': self.group1,
            'group2': self.group2,
            'group3': self.group3
        }

class TotalMonth(Base):
    __tablename__ = 'total_month'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    total = Column(Float, nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'date': self.date.strftime('%Y-%m'),
            'total': self.total
        }

class TotalAll(Base):
    __tablename__ = 'total_all'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    total = Column(Float, nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'total': self.total
        }

# -----------------------------------------------------------------------
# FUNCIONES DE ACTUALIZACIÓN
# -----------------------------------------------------------------------

def update_total_day(session, today, total_sum, sum_group1, sum_group2, sum_group3):
    today_object = session.query(TotalDay).filter_by(date=today).first()

    if today_object is None:
        new_total_day = TotalDay(
            date=today,
            total=total_sum,
            group1=sum_group1,
            group2=sum_group2,
            group3=sum_group3
        )
        session.add(new_total_day)
    else:
        today_object.total += total_sum
        today_object.group1 += sum_group1
        today_object.group2 += sum_group2
        today_object.group3 += sum_group3

def update_total_month(session, month, total_sum):
    if isinstance(month, str):
        month = datetime.strptime(month, '%Y-%m')
    
    month_start = month.replace(day=1).date()
    month_object = session.query(TotalMonth).filter_by(date=month_start).first()

    if month_object is None:
        new_total_month = TotalMonth(date=month_start, total=total_sum)
        session.add(new_total_month)
    else:
        month_object.total += total_sum

def update_total_all(session, total_sum):
    total_object = session.query(TotalAll).first()

    if total_object is None:
        new_total_all = TotalAll(total=total_sum)
        session.add(new_total_all)
    else:
        total_object.total += total_sum

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

        with get_session() as session:
            new_wall_data = WallData(
                date=date_time,
                group=data['group'],
                propeller1=data['propeller1'],
                propeller2=data['propeller2'],
                propeller3=data['propeller3'],
                propeller4=data['propeller4'],
                propeller5=data['propeller5']
            )
            
            new_temp_wall_data = TempWallData(
                date=date_time,
                group=data['group'],
                propeller1=data['propeller1'],
                propeller2=data['propeller2'],
                propeller3=data['propeller3'],
                propeller4=data['propeller4'],
                propeller5=data['propeller5']
            )

            session.add(new_wall_data)
            session.add(new_temp_wall_data)

            sum_group1 = data['propeller1'] + data['propeller2']
            sum_group2 = data['propeller3']
            sum_group3 = data['propeller4'] + data['propeller5']

            update_total_day(session, today, total_sum, sum_group1, sum_group2, sum_group3)
            update_total_month(session, month, total_sum)
            update_total_all(session, total_sum)

            return jsonify(new_wall_data.to_json())

    except Exception as e:
        logger.error(f"Error in create: {e}")
        return jsonify({'error': str(e)}), 500

# ---GET----------------------------------------------------------------

@app.route(BASE_URL + '/readTempLatest/<number>', methods=['GET'])
def readTempLatest(number):
    try:
        with get_session() as session:
            latest_data = session.query(TempWallData).filter_by(group=number).order_by(TempWallData.id.desc()).first()
            
            if latest_data is None:
                return jsonify({'message': 'No data found'}), 404
                
            return jsonify(latest_data.to_json())
    except Exception as e:
        logger.error(f"Error in readTempLatest: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/readLatest', methods=['GET'])
def readLatest():
    try:
        with get_session() as session:
            latest_data = session.query(WallData).order_by(WallData.id.desc()).first()
            
            if latest_data is None:
                return jsonify({'message': 'No data found'}), 404
                
            return jsonify(latest_data.to_json())
    except Exception as e:
        logger.error(f"Error in readLatest: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/readAll', methods=['GET'])
def readAll():
    try:
        with get_session() as session:
            all_data = session.query(WallData).all()
            return jsonify([data.to_json() for data in all_data])
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
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400

        with get_session() as session:
            all_data = session.query(WallData).filter(cast(WallData.date, Date) == date_obj).all()

            hourly_totals = {hour: 0 for hour in range(24)}

            for data in all_data:
                hour = data.date.hour
                total = sum([
                    data.propeller1 ** 2 / 216 * 1000,
                    data.propeller2 ** 2 / 216 * 1000,
                    data.propeller3 ** 2 / 216 * 1000,
                    data.propeller4 ** 2 / 216 * 1000,
                    data.propeller5 ** 2 / 216 * 1000
                ])
                hourly_totals[hour] += total

            return jsonify(hourly_totals)
    except Exception as e:
        logger.error(f"Error in getAllHours: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getAllMinutes', methods=['GET'])
def get_all_minutes():
    try:
        date_str = request.args.get('date')
        if not date_str:
            return jsonify({'error': 'Date parameter is required'}), 400

        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD HH:MM:SS'}), 400

        with get_session() as session:
            all_data = session.query(WallData).filter(
                WallData.date >= date_obj.replace(minute=0, second=0, microsecond=0),
                WallData.date < date_obj.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            ).all()

            minute_totals = {minute: {
                'propeller1': 0,
                'propeller2': 0,
                'propeller3': 0,
                'propeller4': 0,
                'propeller5': 0,
                'total': 0
            } for minute in range(60)}

            for data in all_data:
                minute = data.date.minute
                minute_totals[minute]['propeller1'] += data.propeller1 ** 2 / 216 * 1000
                minute_totals[minute]['propeller2'] += data.propeller2 ** 2 / 216 * 1000
                minute_totals[minute]['propeller3'] += data.propeller3 ** 2 / 216 * 1000
                minute_totals[minute]['propeller4'] += data.propeller4 ** 2 / 216 * 1000
                minute_totals[minute]['propeller5'] += data.propeller5 ** 2 / 216 * 1000
                minute_totals[minute]['total'] += sum([
                    data.propeller1 ** 2 / 216 * 1000,
                    data.propeller2 ** 2 / 216 * 1000,
                    data.propeller3 ** 2 / 216 * 1000,
                    data.propeller4 ** 2 / 216 * 1000,
                    data.propeller5 ** 2 / 216 * 1000
                ])

            return jsonify(minute_totals)
    except Exception as e:
        logger.error(f"Error in getAllMinutes: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getHourByNumber/<number>', methods=['GET'])
def get_hour_by_number(number):
    try:
        today = datetime.now(mexico_tz).date()
        
        with get_session() as session:
            all_data = session.query(WallData).filter(cast(WallData.date, Date) == today).all()

            total = 0
            for data in all_data:
                if data.date.hour == int(number):
                    total += sum([data.propeller1, data.propeller2, data.propeller3, data.propeller4, data.propeller5])

            return jsonify({'hour': number, 'total': total})
    except Exception as e:
        logger.error(f"Error in getHourByNumber: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/get_totals', methods=['GET'])
def get_totals():
    try:
        with get_session() as session:
            results = session.query(
                WallData.group,
                func.sum(
                    WallData.propeller1 +
                    WallData.propeller2 +
                    WallData.propeller3 +
                    WallData.propeller4 +
                    WallData.propeller5
                ).label('total')
            ).group_by(WallData.group).all()

            totals = {f'group{row[0]}': row[1] for row in results}
            return jsonify(totals)
    except Exception as e:
        logger.error(f"Error in get_totals: {e}")
        return jsonify({'error': str(e)}), 500

# GETs | TotalDay -------------------------------------------------------

@app.route(BASE_URL + '/readAllDays', methods=['GET'])
def readAllDays():
    try:
        with get_session() as session:
            all_data = session.query(TotalDay).all()
            return jsonify([data.to_json() for data in all_data])
    except Exception as e:
        logger.error(f"Error in readAllDays: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getCurrentDay', methods=['GET'])
def get_current_day():
    try:
        today = datetime.now(mexico_tz).date()
        
        with get_session() as session:
            today_object = session.query(TotalDay).filter_by(date=today).first()

            if today_object is None:
                return jsonify({'total': 0})
            else:
                return jsonify(today_object.to_json())
    except Exception as e:
        logger.error(f"Error in getCurrentDay: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/read30days', methods=['GET'])
def read30days():
    try:
        today = datetime.now(mexico_tz).date()
        thirty_days_ago = today - timedelta(days=30)
        
        with get_session() as session:
            all_data = session.query(TotalDay).filter(
                TotalDay.date >= thirty_days_ago,
                TotalDay.date <= today
            ).all()

            day_totals = {(thirty_days_ago + timedelta(days=i)).strftime('%d'): 0 for i in range(31)}

            for day in all_data:
                day_totals[day.date.strftime('%d')] = day.total

            return jsonify(day_totals)
    except Exception as e:
        logger.error(f"Error in read30days: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getWeek', methods=['GET'])
def get_week():
    try:
        today = datetime.now(mexico_tz).date()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)

        with get_session() as session:
            week_data = session.query(TotalDay).filter(
                TotalDay.date >= week_start,
                TotalDay.date <= week_end
            ).all()

            week_totals = {day.date.strftime('%A, %Y-%m-%d'): (day.total ** 2 / 216 * 1000) for day in week_data}
            total_week = sum(day.total for day in week_data)

            return jsonify({'week_totals': week_totals, 'total_week': total_week})
    except Exception as e:
        logger.error(f"Error in getWeek: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getDayByNumber/<number>', methods=['GET'])
def get_day_by_number(number):
    try:
        with get_session() as session:
            all_data = session.query(TotalDay).all()

            total = 0
            for data in all_data:
                if data.date.day == int(number):
                    total += data.total

            return jsonify({'day': number, 'total': total})
    except Exception as e:
        logger.error(f"Error in getDayByNumber: {e}")
        return jsonify({'error': str(e)}), 500

# GETs | TotalMonth -----------------------------------------------------

@app.route(BASE_URL + '/getCurrentMonth', methods=['GET'])
def get_current_month():
    try:
        today = datetime.now(mexico_tz).date()
        month_start = today.replace(day=1)
        
        with get_session() as session:
            month_object = session.query(TotalMonth).filter_by(date=month_start).first()

            if month_object is None:
                return jsonify({'total': 0})
            else:
                return jsonify(month_object.to_json())
    except Exception as e:
        logger.error(f"Error in getCurrentMonth: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/readAllMonths', methods=['GET'])
def readAllMonths():
    try:
        with get_session() as session:
            all_data = session.query(TotalMonth).all()

            month_totals = {month: 0 for month in range(1, 13)}

            for data in all_data:
                month = data.date.month
                month_totals[month] += data.total

            return jsonify(month_totals)
    except Exception as e:
        logger.error(f"Error in readAllMonths: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/getMonthsObjects', methods=['GET'])
def get_months_objects():
    try:
        with get_session() as session:
            all_data = session.query(TotalMonth).all()
            return jsonify([data.to_json() for data in all_data])
    except Exception as e:
        logger.error(f"Error in getMonthsObjects: {e}")
        return jsonify({'error': str(e)}), 500

# GETs | TotalAll -------------------------------------------------------

@app.route(BASE_URL + '/getTotal', methods=['GET'])
def get_total():
    try:
        with get_session() as session:
            total_object = session.query(TotalAll).first()

            if total_object is None:
                return jsonify({'total': 0})
            else:
                return jsonify(total_object.to_json())
    except Exception as e:
        logger.error(f"Error in getTotal: {e}")
        return jsonify({'error': str(e)}), 500

# ---DELETE-------------------------------------------------------------

@app.route(BASE_URL + '/resetAll', methods=['DELETE'])
def resetAll():
    try:
        with get_session() as session:
            session.query(WallData).delete()
            session.query(TotalDay).delete()
            session.query(TotalMonth).delete()
            session.query(TotalAll).delete()
            return jsonify({'message': 'All data has been deleted'})
    except Exception as e:
        logger.error(f"Error in resetAll: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/resetTempWallData', methods=['DELETE'])
def resetTempWallData():
    try:
        with get_session() as session:
            session.query(TempWallData).delete()
            return jsonify({'message': 'All temp data has been deleted'})
    except Exception as e:
        logger.error(f"Error in resetTempWallData: {e}")
        return jsonify({'error': str(e)}), 500

@app.route(BASE_URL + '/deleteAllZeros', methods=['DELETE'])
def deleteAllZeros():
    try:
        with get_session() as session:
            session.query(WallData).filter(
                WallData.propeller1 == 0,
                WallData.propeller2 == 0,
                WallData.propeller3 == 0,
                WallData.propeller4 == 0,
                WallData.propeller5 == 0
            ).delete()
            return jsonify({'message': 'All zeros have been deleted'})
    except Exception as e:
        logger.error(f"Error in deleteAllZeros: {e}")
        return jsonify({'error': str(e)}), 500

# -----------------------------------------------------------------------
# CLEANUP
# -----------------------------------------------------------------------

@app.teardown_appcontext
def shutdown_session(exception=None):
    Session.remove()

# -----------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------

if __name__ == '__main__':
    # Crear todas las tablas
    Base.metadata.create_all(engine)
    logger.info("✓ Tablas creadas/verificadas")
    
    # Probar conexión
    try:
        with get_session() as session:
            session.execute('SELECT 1')
        logger.info("✓ Conexión a base de datos exitosa")
    except Exception as e:
        logger.error(f"✗ Error al conectar a la base de datos: {e}")
    
    app.run(debug=False)