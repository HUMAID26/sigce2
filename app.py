import os
import time
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, session, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, func
from werkzeug.security import generate_password_hash, check_password_hash
import smtplib
from email.mime.text import MIMEText
from twilio.rest import Client
import base64
import gevent
from gevent import monkey
monkey.patch_all()

# Load environment variables from a .env file
load_dotenv()

# --- App Configuration ---
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'a_strong_dev_secret_key')
app.config['UPLOAD_FOLDER'] = 'medical_records'

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# --- Database Setup (Works for both local SQLite and deployed PostgreSQL) ---
db_url = os.environ.get('DATABASE_URL')
if db_url and db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)
app.config['SQLALCHEMY_DATABASE_URI'] = db_url or 'sqlite:///clinic.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# --- Extensions Initialization ---
CORS(app, supports_credentials=True)
db = SQLAlchemy(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

# --- Chatbot Configuration ---
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
MODEL_ID = "meta-llama/llama-3-8b-instruct"
API_URL = "https://openrouter.ai/api/v1/chat/completions"

# Rate Limiting for Chatbot
REQUEST_LIMIT = 5
REQUEST_WINDOW = 60  # seconds
request_timestamps = []

# --- Email & SMS Configuration (for notifications) ---
MAIL_USERNAME = os.getenv('MAIL_USERNAME')
MAIL_PASSWORD = os.getenv('MAIL_PASSWORD')
MAIL_SERVER = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
MAIL_PORT = int(os.getenv('MAIL_PORT', 587))
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER')

# --- In-memory Tracking for Sockets ---
user_sids = {}

# --- Database Models ---
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    phone = db.Column(db.String(20))
    password_hash = db.Column(db.String(256), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    is_doctor = db.Column(db.Boolean, default=False)
    appointments = db.relationship('Appointment', backref='patient', lazy=True, cascade="all, delete-orphan")
    sent_messages = db.relationship('Message', foreign_keys='Message.sender_id', backref='sender', lazy=True, cascade="all, delete-orphan")
    received_messages = db.relationship('Message', foreign_keys='Message.recipient_id', backref='recipient', lazy=True, cascade="all, delete-orphan")
    medical_records = db.relationship('MedicalRecord', backref='patient_record', lazy=True, cascade="all, delete-orphan")

    def to_dict(self):
        return {"id": self.id, "name": self.name, "email": self.email, "phone": self.phone, "isAdmin": self.is_admin, "isDoctor": self.is_doctor}

class Doctor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    specialty = db.Column(db.String(100), nullable=False)
    experience = db.Column(db.Integer, nullable=False)
    qualifications = db.Column(db.String(255))
    about = db.Column(db.Text)
    photo = db.Column(db.Text)
    appointments = db.relationship('Appointment', backref='doctor', lazy=True, cascade="all, delete-orphan")
    schedules = db.relationship('Schedule', backref='doctor', lazy=True, cascade="all, delete-orphan")
    medical_records = db.relationship('MedicalRecord', backref='doctor', lazy=True, cascade="all, delete-orphan")

    def to_dict(self):
        return {"id": self.id, "name": self.name, "specialty": self.specialty, "experience": self.experience, "photo": self.photo, "qualifications": self.qualifications, "about": self.about}

class Schedule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    doctor_id = db.Column(db.Integer, db.ForeignKey('doctor.id', ondelete="CASCADE"), nullable=False)
    day_of_week = db.Column(db.Integer, nullable=False) # 0=Monday, 6=Sunday
    start_time = db.Column(db.String(5), nullable=False) # e.g., "09:00"
    end_time = db.Column(db.String(5), nullable=False) # e.g., "17:00"

class Service(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    price = db.Column(db.Float, nullable=False)
    appointments = db.relationship('Appointment', backref='service', lazy=True, cascade="all, delete-orphan")

    def to_dict(self):
        return {"id": self.id, "name": self.name, "description": self.description, "price": self.price}
        
class Appointment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete="CASCADE"), nullable=False)
    doctor_id = db.Column(db.Integer, db.ForeignKey('doctor.id', ondelete="CASCADE"), nullable=False)
    service_id = db.Column(db.Integer, db.ForeignKey('service.id', ondelete="CASCADE"), nullable=False)
    date = db.Column(db.String(20), nullable=False)
    time = db.Column(db.String(10), nullable=False)
    symptoms = db.Column(db.Text)
    status = db.Column(db.String(20), default='pending')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        return {
            "id": self.id, "userId": self.user_id, "doctorId": self.doctor_id, "serviceId": self.service_id,
            "doctorName": self.doctor.name if self.doctor else "N/A",
            "patientName": self.patient.name if self.patient else "N/A",
            "patientEmail": self.patient.email if self.patient else "N/A",
            "date": self.date, "time": self.time, "symptoms": self.symptoms, "status": self.status,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "serviceName": self.service.name if self.service else "N/A",
            "price": self.service.price if self.service else "N/A"
        }

class Message(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sender_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete="CASCADE"), nullable=False)
    recipient_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete="CASCADE"), nullable=False)
    content = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    message_type = db.Column(db.String(20), default='general') # 'general' or 'appointment'

    def to_dict(self):
        return {
            "id": self.id, "senderId": self.sender_id, "senderName": self.sender.name,
            "recipientId": self.recipient_id, "content": self.content, "timestamp": self.timestamp.isoformat(),
            "messageType": self.message_type
        }

class MedicalRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    patient_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete="CASCADE"), nullable=False)
    doctor_id = db.Column(db.Integer, db.ForeignKey('doctor.id', ondelete="CASCADE"), nullable=False)
    record_type = db.Column(db.String(50), nullable=False) # e.g., 'Prescription', 'Lab Result'
    file_path = db.Column(db.String(255), nullable=False)
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id, "patientId": self.patient_id, "doctorId": self.doctor_id,
            "recordType": self.record_type, "filePath": self.file_path,
            "doctorName": self.doctor.name, "uploadedAt": self.uploaded_at.isoformat()
        }

# --- Database Initialization ---
with app.app_context():
    db.create_all()
    if not User.query.filter_by(email='admin@sigceclinic.com').first():
        admin_user = User(name='Admin', email='admin@sigceclinic.com', password_hash=generate_password_hash('admin123'), is_admin=True)
        db.session.add(admin_user)
        db.session.commit()
    if not Service.query.first():
        db.session.add_all([
            Service(name='General Consultation', description='A standard check-up with a general practitioner.', price=500.00),
            Service(name='Specialist Visit', description='Consultation with a specialist doctor.', price=1500.00),
            Service(name='Routine Dental Check-up', description='Cleaning and examination of teeth and gums.', price=750.00),
            Service(name='Cardiology Evaluation', description='Evaluation and diagnosis of heart conditions.', price=2000.00),
        ])
        db.session.commit()

# --- Notification Helpers ---
def send_email_notification(recipient_email, subject, body):
    if not all([MAIL_USERNAME, MAIL_PASSWORD]):
        print("Email credentials not set. Skipping email notification.")
        return False
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = MAIL_USERNAME
        msg['To'] = recipient_email
        with smtplib.SMTP(MAIL_SERVER, MAIL_PORT) as server:
            server.starttls()
            server.login(MAIL_USERNAME, MAIL_PASSWORD)
            server.sendmail(MAIL_USERNAME, recipient_email, msg.as_string())
        print(f"Email sent to {recipient_email}")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

def send_sms_notification(recipient_phone, body):
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        print("Twilio credentials not set. Skipping SMS notification.")
        return False
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        message = client.messages.create(
            to=recipient_phone,
            from_=TWILIO_PHONE_NUMBER,
            body=body
        )
        print(f"SMS sent to {recipient_phone}, SID: {message.sid}")
        return True
    except Exception as e:
        print(f"Failed to send SMS: {e}")
        return False

# --- Chatbot Helper Functions ---
def check_rate_limit():
    global request_timestamps
    current_time = time.time()
    request_timestamps = [t for t in request_timestamps if current_time - t < REQUEST_WINDOW]
    if len(request_timestamps) >= REQUEST_LIMIT:
        wait_time = REQUEST_WINDOW - (current_time - request_timestamps[0])
        return False, wait_time
    return True, 0

def get_chatbot_response(prompt):
    allowed, wait_time = check_rate_limit()
    if not allowed:
        return None, f"Rate limit exceeded. Please wait {int(wait_time)} seconds."
    
    # Custom, non-LLM responses
    custom_responses = {
        "hello": "Welcome to SIGCE Clinic. I am your virtual assistant. How may I help you today?",
        "bye": "Thank you for contacting SIGCE Clinic. Have a healthy day!",
        "location": "SIGCE Clinic is located at 123 Health Street, Medical City.",
        "hours": "Our clinic operates 24/7 for emergency services. For appointments, please check the specific doctor's schedule on our website.",
        "contact": "You can contact us via the 'Contact' page or book an appointment directly with a doctor."
    }
    if prompt.lower() in custom_responses:
        return custom_responses[prompt.lower()], None

    # Dynamic responses for doctors and services
    doctors = Doctor.query.all()
    doctor_names = [d.name.lower() for d in doctors]
    if any(name in prompt.lower() for name in doctor_names):
        doctor = next((d for d in doctors if d.name.lower() in prompt.lower()), None)
        if doctor:
            return f"Dr. {doctor.name} is a {doctor.specialty} with {doctor.experience} years of experience. You can book an appointment on our website.", None

    services = Service.query.all()
    service_names = [s.name.lower() for s in services]
    if any(name in prompt.lower() for name in service_names):
        service = next((s for s in services if s.name.lower() in prompt.lower()), None)
        if service:
            return f"{service.name} costs ${service.price:.2f}. {service.description}", None

    # Fallback to LLM for other queries
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": request.host_url,
        "X-Title": "SIGCE Clinic Bot"
    }
    payload = {
        "model": MODEL_ID,
        "messages": [
            {"role": "system", "content": f"You are a professional virtual assistant for SIGCE Clinic. The current date is {datetime.now().strftime('%A, %B %d, %Y')}. Be helpful and concise."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 150
    }
    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        request_timestamps.append(time.time())
        return response.json()["choices"][0]["message"]["content"], None
    except requests.exceptions.RequestException as e:
        return None, f"API request failed: {str(e)}"

# --- Main Route ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/download/<path:filename>')
def download_file(filename):
    if 'user_id' not in session:
        return jsonify({'error': 'Authentication required'}), 401
    
    record = MedicalRecord.query.filter_by(file_path=filename).first()
    if not record or (record.patient_id != session['user_id'] and not session['is_admin']):
        return jsonify({'error': 'Permission denied'}), 403
    
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


# --- API Routes ---

@app.route('/api/chatbot', methods=['POST'])
def chat():
    user_input = request.json.get('message', '').strip()
    if not user_input:
        return jsonify({"error": "Empty message received."}), 400
    
    response, error = get_chatbot_response(user_input)
    if error:
        status_code = 429 if "Rate limit" in error else 500
        return jsonify({"error": error}), status_code
    return jsonify({"response": response})

# --- User & Session Management ---
@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    if not data: return jsonify({'error': 'No data provided'}), 400
    user = User.query.filter_by(email=data['email']).first()
    if user and check_password_hash(user.password_hash, data['password']):
        if user.id in user_sids:
            old_sid = user_sids[user.id]
            socketio.emit('force_logout', {'msg': 'This account has been signed in from another device.'}, room=old_sid)
        session['user_id'] = user.id
        session['is_admin'] = user.is_admin
        return jsonify({'id': user.id, 'name': user.name, 'email': user.email, 'phone': user.phone, 'isAdmin': user.is_admin})
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/api/signup', methods=['POST'])
def signup():
    try:
        data = request.get_json()
        if not data: return jsonify({'error': 'No data provided'}), 400
        if User.query.filter_by(email=data['email']).first(): return jsonify({'error': 'Email already exists'}), 409
        new_user = User(
            name=data['name'], email=data['email'], phone=data.get('phone', ''),
            password_hash=generate_password_hash(data['password'])
        )
        db.session.add(new_user)
        db.session.commit()
        session['user_id'] = new_user.id
        session['is_admin'] = new_user.is_admin
        return jsonify({'id': new_user.id, 'name': new_user.name, 'email': new_user.email, 'phone': new_user.phone, 'isAdmin': new_user.is_admin}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/api/logout', methods=['POST'])
def logout():
    user_id = session.get('user_id')
    if user_id in user_sids:
        del user_sids[user_id]
    session.clear()
    if request.args.get('silent'): return '', 204
    return jsonify({'message': 'Logged out successfully'}), 200

@app.route('/api/session', methods=['GET'])
def get_session():
    if 'user_id' in session:
        user = User.query.get(session['user_id'])
        if user:
            return jsonify(user.to_dict())
    return jsonify({'error': 'Not authenticated'}), 401

@app.route('/api/profile', methods=['GET', 'PUT'])
def handle_profile():
    if 'user_id' not in session: return jsonify({'error': 'Authentication required'}), 401
    user = User.query.get(session['user_id'])
    if request.method == 'GET':
        return jsonify(user.to_dict())
    if request.method == 'PUT':
        data = request.get_json()
        user.name = data.get('name', user.name)
        user.phone = data.get('phone', user.phone)
        db.session.commit()
        return jsonify(user.to_dict())

# --- Doctor Management ---
@app.route('/api/doctors', methods=['GET', 'POST'])
def handle_doctors():
    if request.method == 'GET':
        return jsonify([doc.to_dict() for doc in Doctor.query.all()])
    if request.method == 'POST':
        if not session.get('is_admin'): return jsonify({'error': 'Admin access required'}), 403
        data = request.get_json()
        new_doctor = Doctor(name=data['name'], specialty=data['specialty'], experience=data['experience'], photo=data.get('photo'), qualifications=data.get('qualifications'), about=data.get('about'))
        db.session.add(new_doctor)
        db.session.commit()
        # REAL-TIME UPDATE: Broadcast the new doctor to all clients
        socketio.emit('doctor_update', {'action': 'add', 'doctor': new_doctor.to_dict()})
        return jsonify(new_doctor.to_dict()), 201

@app.route('/api/doctors/<int:doctor_id>', methods=['GET', 'PUT', 'DELETE'])
def handle_doctor(doctor_id):
    doctor = Doctor.query.get_or_404(doctor_id)
    if request.method == 'GET': return jsonify(doctor.to_dict())
    if not session.get('is_admin'): return jsonify({'error': 'Admin access required'}), 403
    if request.method == 'PUT':
        data = request.get_json()
        doctor.name = data.get('name', doctor.name)
        doctor.specialty = data.get('specialty', doctor.specialty)
        doctor.experience = data.get('experience', doctor.experience)
        doctor.qualifications = data.get('qualifications', doctor.qualifications)
        doctor.about = data.get('about', doctor.about)
        if 'photo' in data: doctor.photo = data['photo']
        db.session.commit()
        # REAL-TIME UPDATE: Broadcast the updated doctor to all clients
        socketio.emit('doctor_update', {'action': 'update', 'doctor': doctor.to_dict()})
        return jsonify(doctor.to_dict())
    if request.method == 'DELETE':
        db.session.delete(doctor)
        db.session.commit()
        # REAL-TIME UPDATE: Broadcast the deletion to all clients
        socketio.emit('doctor_update', {'action': 'delete', 'doctorId': doctor_id})
        return jsonify({'message': 'Doctor deleted successfully'}), 200

# --- Doctor Schedules ---
@app.route('/api/doctors/<int:doctor_id>/schedule', methods=['GET', 'POST'])
def handle_doctor_schedule(doctor_id):
    if request.method == 'GET':
        schedule = Schedule.query.filter_by(doctor_id=doctor_id).all()
        return jsonify([{"day_of_week": s.day_of_week, "start_time": s.start_time, "end_time": s.end_time} for s in schedule])
    if request.method == 'POST':
        if not session.get('is_admin'): return jsonify({'error': 'Admin access required'}), 403
        data = request.get_json()
        db.session.query(Schedule).filter_by(doctor_id=doctor_id).delete()
        for day in data.get('schedule', []):
            new_schedule = Schedule(doctor_id=doctor_id, day_of_week=day['day_of_week'], start_time=day['start_time'], end_time=day['end_time'])
            db.session.add(new_schedule)
        db.session.commit()
        # REAL-TIME UPDATE: Broadcast the updated schedule
        schedule_data = [{"day_of_week": s.day_of_week, "start_time": s.start_time, "end_time": s.end_time} for s in Schedule.query.filter_by(doctor_id=doctor_id).all()]
        socketio.emit('schedule_update', {'doctorId': doctor_id, 'schedule': schedule_data})
        return jsonify({"message": "Schedule updated"}), 200

# --- Service Management ---
@app.route('/api/services', methods=['GET', 'POST'])
def handle_services():
    if request.method == 'GET':
        return jsonify([s.to_dict() for s in Service.query.all()])
    if request.method == 'POST':
        if not session.get('is_admin'): return jsonify({'error': 'Admin access required'}), 403
        data = request.get_json()
        new_service = Service(name=data['name'], description=data.get('description'), price=data['price'])
        db.session.add(new_service)
        db.session.commit()
        # REAL-TIME UPDATE: Broadcast the new service to all clients
        socketio.emit('service_update', {'action': 'add', 'service': new_service.to_dict()})
        return jsonify(new_service.to_dict()), 201

@app.route('/api/services/<int:service_id>', methods=['PUT', 'DELETE'])
def update_delete_service(service_id):
    if not session.get('is_admin'):
        return jsonify({'error': 'Admin access required'}), 403
    
    service = Service.query.get_or_404(service_id)
    
    if request.method == 'PUT':
        data = request.get_json()
        service.name = data.get('name', service.name)
        service.description = data.get('description', service.description)
        service.price = data.get('price', service.price)
        db.session.commit()
        # REAL-TIME UPDATE: Broadcast the updated service to all clients
        socketio.emit('service_update', {'action': 'update', 'service': service.to_dict()})
        return jsonify(service.to_dict())
        
    if request.method == 'DELETE':
        try:
            db.session.delete(service)
            db.session.commit()
            
            # REAL-TIME UPDATE: Broadcast the deletion to all clients
            socketio.emit('service_update', {'action': 'delete', 'serviceId': service_id})
            return '', 204
        
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 500

# --- Appointment Management ---
@app.route('/api/appointments', methods=['GET', 'POST'])
def handle_appointments():
    if 'user_id' not in session: return jsonify({'error': 'Authentication required'}), 401
    user = User.query.get(session['user_id'])
    if request.method == 'GET':
        appointments = Appointment.query.order_by(Appointment.date.desc()).all() if user.is_admin else Appointment.query.filter_by(user_id=user.id).order_by(Appointment.date.desc()).all()
        return jsonify([apt.to_dict() for apt in appointments])
    if request.method == 'POST':
        if user.is_admin: return jsonify({'error': 'Admin cannot book appointments'}), 403
        data = request.get_json()
        new_appointment = Appointment(user_id=user.id, doctor_id=data['doctorId'], service_id=data['serviceId'], date=data['date'], time=data['time'], symptoms=data.get('symptoms', ''))
        db.session.add(new_appointment)
        db.session.commit()
        # REAL-TIME UPDATE: Emit to admin room for new appointment
        socketio.emit('new_appointment', new_appointment.to_dict(), room='admin_room')
        return jsonify(new_appointment.to_dict()), 201
    
@app.route('/api/appointments/booked_slots', methods=['GET'])
def get_booked_slots():
    doctor_id = request.args.get('doctorId', type=int)
    date = request.args.get('date')
    if not doctor_id or not date:
        return jsonify({"error": "Doctor ID and date are required"}), 400

    booked_appointments = Appointment.query.filter_by(doctor_id=doctor_id, date=date, status='confirmed').all()
    booked_times = [apt.time for apt in booked_appointments]
    return jsonify(booked_times)

@app.route('/api/appointments/<int:apt_id>/status', methods=['PUT'])
def update_appointment_status(apt_id):
    if 'user_id' not in session: return jsonify({'error': 'Authentication required'}), 401
    appointment, data = Appointment.query.get_or_404(apt_id), request.get_json()
    new_status = data.get('status')
    if not new_status: return jsonify({'error': 'Status is required'}), 400
    is_admin, is_owner = session.get('is_admin'), appointment.user_id == session.get('user_id')
    if not (is_admin or (is_owner and new_status == 'cancelled')): return jsonify({'error': 'Permission denied'}), 403
    appointment.status = new_status
    db.session.commit()
    
    # Send notification
    if appointment.patient:
        subject = f"Appointment {new_status.capitalize()}"
        body = f"Hello {appointment.patient.name},\n\nYour appointment with Dr. {appointment.doctor.name} on {appointment.date} at {appointment.time} has been {new_status}. Please check your dashboard for details."
        send_email_notification(appointment.patient.email, subject, body)
        if appointment.patient.phone:
            send_sms_notification(appointment.patient.phone, f"Your SIGCE Clinic appointment with Dr. {appointment.doctor.name} on {appointment.date} at {appointment.time} has been {new_status}.")

    # REAL-TIME UPDATE: Notify the patient and admin
    user_sid = user_sids.get(appointment.user_id)
    if user_sid:
        socketio.emit('appointment_update', {'appointmentId': appointment.id, 'status': new_status, 'message': f'Your appointment has been {new_status}'}, room=user_sid)
    socketio.emit('appointment_update', {'appointmentId': appointment.id, 'status': new_status, 'message': f'Appointment {apt_id} status updated to {new_status}'}, room='admin_room')
    
    return jsonify(appointment.to_dict())

# --- Messages ---
@app.route('/api/messages', methods=['GET', 'POST'])
def handle_messages():
    if 'user_id' not in session: return jsonify({'error': 'Authentication required'}), 401
    user_id = session['user_id']
    is_admin = session.get('is_admin')
    
    if request.method == 'GET':
        if is_admin:
            messages = Message.query.order_by(Message.sender_id, Message.timestamp).all()
            conversations = {}
            for msg in messages:
                other_user_id = msg.sender_id if msg.sender_id != user_id else msg.recipient_id
                if other_user_id not in conversations:
                    other_user = User.query.get(other_user_id)
                    conversations[other_user_id] = {"with_user_id": other_user_id, "with_user_name": other_user.name, "messages": []}
                conversations[other_user_id]["messages"].append(msg.to_dict())
            return jsonify(list(conversations.values()))
        else:
            admin = User.query.filter_by(is_admin=True).first()
            messages = Message.query.filter(or_((Message.sender_id == user_id) & (Message.recipient_id == admin.id), (Message.sender_id == admin.id) & (Message.recipient_id == user_id))).order_by(Message.timestamp.asc()).all()
            return jsonify([msg.to_dict() for msg in messages])
            
    if request.method == 'POST':
        data = request.get_json()
        sender_id = session['user_id']
        recipient_id = data.get('recipient_id')
        content = data.get('content')
        
        if not content: return jsonify({'error': 'Content is required'}), 400
        
        if is_admin:
            if not recipient_id: return jsonify({'error': 'Recipient is required'}), 400
            message_type = 'reply'
        else:
            admin = User.query.filter_by(is_admin=True).first()
            if not admin: return jsonify({'error': 'No admin user found'}), 500
            recipient_id = admin.id
            message_type = 'general'
            
        new_message = Message(sender_id=sender_id, recipient_id=recipient_id, content=content, message_type=message_type)
        db.session.add(new_message)
        db.session.commit()
        
        # REAL-TIME UPDATE: Notify the recipient and sender
        recipient_sid = user_sids.get(recipient_id)
        if recipient_sid:
            socketio.emit('new_message', new_message.to_dict(), room=recipient_sid)
        
        sender_sid = user_sids.get(sender_id)
        if sender_sid and sender_id != recipient_id:
            socketio.emit('new_message', new_message.to_dict(), room=sender_sid)
            
        return jsonify(new_message.to_dict()), 201

@app.route('/api/messages/<int:message_id>', methods=['DELETE'])
def delete_message(message_id):
    if 'user_id' not in session: return jsonify({'error': 'Authentication required'}), 401
    message = Message.query.get_or_404(message_id)
    user_id = session['user_id']
    if user_id != message.sender_id and user_id != message.recipient_id and not session.get('is_admin'):
        return jsonify({'error': 'Permission denied'}), 403
    db.session.delete(message)
    db.session.commit()
    # REAL-TIME UPDATE: Notify both sender and recipient of deletion
    socketio.emit('message_deleted', {'messageId': message_id, 'senderId': message.sender_id, 'recipientId': message.recipient_id}, room=user_sids.get(message.sender_id))
    if message.sender_id != message.recipient_id:
        socketio.emit('message_deleted', {'messageId': message_id, 'senderId': message.sender_id, 'recipientId': message.recipient_id}, room=user_sids.get(message.recipient_id))

    return jsonify({'message': 'Message deleted successfully'}), 200

# --- Medical Records ---
@app.route('/api/medical_records', methods=['GET', 'POST'])
def handle_medical_records():
    if 'user_id' not in session:
        return jsonify({'error': 'Authentication required'}), 401
    
    if request.method == 'GET':
        records = MedicalRecord.query.filter_by(patient_id=session['user_id']).order_by(MedicalRecord.uploaded_at.desc()).all()
        return jsonify([r.to_dict() for r in records])
    
    if request.method == 'POST':
        if not session.get('is_admin'):
            return jsonify({'error': 'Admin access required'}), 403
        
        data = request.get_json()
        file_data = data.get('fileData')
        file_name = data.get('fileName')
        
        if not all([data.get('patientId'), data.get('doctorId'), data.get('recordType'), file_data, file_name]):
            return jsonify({'error': 'Missing required fields'}), 400

        try:
            # Decode the base64 file data
            header, encoded = file_data.split(',', 1)
            file_bytes = base64.b64decode(encoded)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
            
            with open(file_path, 'wb') as f:
                f.write(file_bytes)

            new_record = MedicalRecord(
                patient_id=data['patientId'],
                doctor_id=data['doctorId'],
                record_type=data['recordType'],
                file_path=file_name
            )
            db.session.add(new_record)
            db.session.commit()

            # REAL-TIME UPDATE: Notify the patient and admin
            patient_sid = user_sids.get(new_record.patient_id)
            if patient_sid:
                socketio.emit('medical_record_update', new_record.to_dict(), room=patient_sid)
            socketio.emit('medical_record_update', new_record.to_dict(), room='admin_room')
            
            return jsonify(new_record.to_dict()), 201
        except Exception as e:
            db.session.rollback()
            return jsonify({'error': str(e)}), 500

# --- Admin Analytics ---
@app.route('/api/analytics', methods=['GET'])
def get_analytics():
    if not session.get('is_admin'): return jsonify({'error': 'Admin access required'}), 403
    
    total_appointments = Appointment.query.count()
    completed_appointments = Appointment.query.filter_by(status='completed').count()
    
    appointments_by_specialty = db.session.query(Doctor.specialty, func.count(Appointment.id)).join(Appointment).group_by(Doctor.specialty).all()
    appointments_by_specialty_data = [{"specialty": s, "count": c} for s, c in appointments_by_specialty]
    
    daily_appointments = db.session.query(Appointment.date, func.count(Appointment.id)).group_by(Appointment.date).order_by(Appointment.date).all()
    daily_appointments_data = [{"date": d, "count": c} for d, c in daily_appointments]
    
    return jsonify({
        "totalAppointments": total_appointments,
        "completedAppointments": completed_appointments,
        "appointmentsBySpecialty": appointments_by_specialty_data,
        "dailyAppointments": daily_appointments_data
    })

# --- Admin User Management ---
@app.route('/api/users', methods=['GET'])
def get_users():
    if not session.get('is_admin'): return jsonify({'error': 'Admin access required'}), 403
    users = User.query.all()
    return jsonify([user.to_dict() for user in users])

@app.route('/api/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    if not session.get('is_admin'): return jsonify({'error': 'Admin access required'}), 403
    user = User.query.get_or_404(user_id)
    if user.is_admin: return jsonify({'error': 'Cannot delete an admin user'}), 403
    db.session.delete(user)
    db.session.commit()
    
    # REAL-TIME UPDATE: Notify all clients about user deletion
    socketio.emit('user_deleted', {'userId': user_id})
    
    return jsonify({'message': 'User deleted successfully'}), 200

# --- Socket.IO Event Handlers ---
@socketio.on('connect')
def handle_connect():
    user_id = session.get('user_id')
    if user_id:
        user_sids[user_id] = request.sid
        user = User.query.get(user_id)
        if user and user.is_admin:
            join_room('admin_room')
        print(f"User {user_id} connected with SID: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    user_id_to_remove = next((user_id for user_id, sid in user_sids.items() if sid == request.sid), None)
    if user_id_to_remove:
        if user_id_to_remove in user_sids and user_sids[user_id_to_remove] == request.sid:
            del user_sids[user_id_to_remove]
        print(f"User {user_id_to_remove} disconnected")

# --- Error Handlers & Main Execution ---
@app.errorhandler(404)
def not_found(error): return jsonify({'error': 'Resource not found'}), 404
@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
