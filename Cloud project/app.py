"""
Movie Ticket Booking System using Flask and AWS
Author: Antigravity AI
Description: A complete cloud-native web application demonstrating Flask, AWS DynamoDB, SNS, and IAM roles.
"""

import os
import uuid
import logging
from flask import Flask, render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash, check_password_hash
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'cyberpunk_secret_key_102938')

# ==========================================
# AWS CONFIGURATION & RESILIENT INITIALIZATION
# ==========================================

AWS_ENABLED = True
SNS_TOPIC_ARN = None

# In-memory Local Database Fallback (for local testing without AWS configured)
MOCK_USERS = {}
MOCK_MOVIES = {}
MOCK_BOOKINGS = {}

# Set up default credentials or IAM role detection
try:
    # Use standard us-east-1 as default region if not defined
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-south-1')
    
    # Try to initialize clients with default credential chain (IAM Role / AWS CLI profile)
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    sns_client = boto3.client('sns', region_name=aws_region)
    
    # Run a simple query to verify credentials are active
    sts_client = boto3.client('sts', region_name=aws_region)
    sts_client.get_caller_identity()
    
    logger.info("AWS Credentials verified successfully. Running in REAL AWS MODE.")
except (NoCredentialsError, ClientError, Exception) as e:
    AWS_ENABLED = False
    logger.warning(f"AWS initialization failed: {e}. Falling back to SANDBOX MOCK MODE (in-memory).")

# ==========================================
# DATABASE INITIALIZATION AND SEED DATA
# ==========================================

DEFAULT_MOVIES = [
    {
        "movie_name": "Dune: Part Two",
        "theater": "IMAX Laser - Screen 1",
        "timing": "18:00",
        "ticket_price": 15,
        "image_url": "https://images.unsplash.com/photo-1451187580459-43490279c0fa?w=800"
    },
    {
        "movie_name": "Oppenheimer",
        "theater": "Dolby Atmos - Screen 3",
        "timing": "20:30",
        "ticket_price": 12,
        "image_url": "https://images.unsplash.com/photo-1518156677180-95a2893f3e9f?w=800"
    },
    {
        "movie_name": "Spider-Man: Across the Spider-Verse",
        "theater": "Premium 3D - Screen 2",
        "timing": "15:00",
        "ticket_price": 10,
        "image_url": "https://images.unsplash.com/photo-1579783900882-c0d3dad7b119?w=800"
    },
    {
        "movie_name": "Interstellar",
        "theater": "Director's Recliners - Screen 4",
        "timing": "22:15",
        "ticket_price": 18,
        "image_url": "https://images.unsplash.com/photo-1506703719100-a0f3a48c0f86?w=800"
    }
]

def initialize_aws_resources():
    """Initializes tables and SNS topic if AWS is enabled."""
    global SNS_TOPIC_ARN
    if not AWS_ENABLED:
        # Seed mock data in memory
        for mov in DEFAULT_MOVIES:
            m_id = str(uuid.uuid4())
            MOCK_MOVIES[m_id] = {
                "movie_id": m_id,
                **mov
            }
        logger.info("Sandbox database successfully seeded with default movies.")
        return

    # 1. Initialize DynamoDB Tables
    try:
        existing_tables = [table.name for table in dynamodb.tables.all()]
    except Exception as e:
        logger.error(f"Failed to list tables: {e}. Switching to Sandbox Mode.")
        globals()['AWS_ENABLED'] = False
        initialize_aws_resources()
        return

    # Helper function to create a DynamoDB table
    def create_table(name, pk):
        if name not in existing_tables:
            try:
                logger.info(f"Creating table '{name}'...")
                table = dynamodb.create_table(
                    TableName=name,
                    KeySchema=[{'AttributeName': pk, 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': pk, 'AttributeType': 'S'}],
                    ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                )
                table.wait_until_exists()
                logger.info(f"Table '{name}' is active.")
            except ClientError as e:
                logger.error(f"Error creating table {name}: {e}")
        else:
            logger.info(f"Table '{name}' already exists.")

    create_table('Userss', 'email')
    create_table('Movies', 'email')
    create_table('Bookings', 'email')

    # Seed default movies if the Movies table is empty
    try:
        movies_table = dynamodb.Table('Movies')
        scan_resp = movies_table.scan(Limit=1)
        if not scan_resp.get('Items'):
            logger.info("Seeding DynamoDB with sample movies...")
            for mov in DEFAULT_MOVIES:
                movie_id = str(uuid.uuid4())
                movies_table.put_item(
                    Item={
                        'movie_id': movie_id,
                        'movie_name': mov['movie_name'],
                        'theater': mov['theater'],
                        'timing': mov['timing'],
                        'ticket_price': int(mov['ticket_price']),
                        'image_url': mov['image_url']
                    }
                )
            logger.info("DynamoDB Movies table seeded.")
    except Exception as e:
        logger.error(f"Error seeding Movies table: {e}")

    # 2. Initialize SNS Topic
    try:
        logger.info("Locating or creating SNS Topic 'MovieTicketBookingNotifications'...")
        topic_resp = sns_client.create_topic(Name='cinesns')
        SNS_TOPIC_ARN = topic_resp['TopicArn']
        logger.info(f"SNS Topic active: {SNS_TOPIC_ARN}")
    except Exception as e:
        logger.error(f"Error initializing SNS topic: {e}")

# Run AWS Initializer
initialize_aws_resources()

# ==========================================
# DATABASE ABSTRACTION LAYER (API)
# ==========================================

def get_user_by_email(email):
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Users')
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('email').eq(email)
            )
            items = response.get('Items', [])
            return items[0] if items else None
        except Exception as e:
            logger.error(f"DynamoDB get_user_by_email error: {e}")
            return None
    else:
        for u in MOCK_USERS.values():
            if u['email'].lower() == email.lower():
                return u
        return None

def create_user(name, email, password_hash):
    user_id = str(uuid.uuid4())
    user_data = {
        'user_id': user_id,
        'name': name,
        'email': email,
        'password': password_hash
    }
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Users')
            table.put_item(Item=user_data)
        except Exception as e:
            logger.error(f"DynamoDB create_user error: {e}")
            return None
    else:
        MOCK_USERS[user_id] = user_data
    
    # Subscribe user to SNS topic
    subscribe_email(email)
    return user_data

def get_movies():
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Movies')
            return table.scan().get('Items', [])
        except Exception as e:
            logger.error(f"DynamoDB get_movies error: {e}")
            return []
    else:
        return list(MOCK_MOVIES.values())

def get_movie(movie_id):
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Movies')
            response = table.get_item(Key={'movie_id': movie_id})
            return response.get('Item', None)
        except Exception as e:
            logger.error(f"DynamoDB get_movie error: {e}")
            return None
    else:
        return MOCK_MOVIES.get(movie_id, None)

def create_movie(movie_name, theater, timing, ticket_price, image_url):
    movie_id = str(uuid.uuid4())
    movie_data = {
        'movie_id': movie_id,
        'movie_name': movie_name,
        'theater': theater,
        'timing': timing,
        'ticket_price': int(ticket_price),
        'image_url': image_url or "https://images.unsplash.com/photo-1489599849927-2ee91cede3ba?w=800"
    }
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Movies')
            table.put_item(Item=movie_data)
        except Exception as e:
            logger.error(f"DynamoDB create_movie error: {e}")
            return None
    else:
        MOCK_MOVIES[movie_id] = movie_data
    return movie_data

def update_movie(movie_id, movie_name, theater, timing, ticket_price, image_url):
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Movies')
            table.update_item(
                Key={'movie_id': movie_id},
                UpdateExpression="set movie_name=:n, theater=:th, timing=:ti, ticket_price=:p, image_url=:u",
                ExpressionAttributeValues={
                    ':n': movie_name,
                    ':th': theater,
                    ':ti': timing,
                    ':p': int(ticket_price),
                    ':u': image_url
                }
            )
            return True
        except Exception as e:
            logger.error(f"DynamoDB update_movie error: {e}")
            return False
    else:
        if movie_id in MOCK_MOVIES:
            MOCK_MOVIES[movie_id].update({
                'movie_name': movie_name,
                'theater': theater,
                'timing': timing,
                'ticket_price': int(ticket_price),
                'image_url': image_url
            })
            return True
        return False

def delete_movie(movie_id):
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Movies')
            table.delete_item(Key={'movie_id': movie_id})
            return True
        except Exception as e:
            logger.error(f"DynamoDB delete_movie error: {e}")
            return False
    else:
        if movie_id in MOCK_MOVIES:
            del MOCK_MOVIES[movie_id]
            return True
        return False

def create_booking(user_id, movie_id, seats, total_price):
    booking_id = str(uuid.uuid4())
    booking_data = {
        'booking_id': booking_id,
        'user_id': user_id,
        'movie_id': movie_id,
        'seats': int(seats),
        'total_price': int(total_price),
        'status': 'Requested'
    }
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Bookings')
            table.put_item(Item=booking_data)
        except Exception as e:
            logger.error(f"DynamoDB create_booking error: {e}")
            return None
    else:
        MOCK_BOOKINGS[booking_id] = booking_data
    
    # Notify new booking
    movie = get_movie(movie_id)
    movie_name = movie['movie_name'] if movie else "Unknown Movie"
    subject = f"New Movie Booking Requested - ID: {booking_id[:8]}"
    message = (
        f"Hello Space-Traveler!\n\n"
        f"Your booking request has been successfully placed.\n\n"
        f"Booking Reference: #{booking_id}\n"
        f"Movie: {movie_name}\n"
        f"Seats Reserved: {seats}\n"
        f"Total Cost: ${total_price}\n"
        f"Current Status: Requested\n\n"
        f"Please wait while our administrators confirm your theater tickets."
    )
    publish_notification(subject, message)
    return booking_data

def get_bookings_by_user(user_id):
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Bookings')
            response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('user_id').eq(user_id)
            )
            return response.get('Items', [])
        except Exception as e:
            logger.error(f"DynamoDB get_bookings_by_user error: {e}")
            return []
    else:
        return [b for b in MOCK_BOOKINGS.values() if b['user_id'] == user_id]

def get_all_bookings():
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Bookings')
            return table.scan().get('Items', [])
        except Exception as e:
            logger.error(f"DynamoDB get_all_bookings error: {e}")
            return []
    else:
        return list(MOCK_BOOKINGS.values())

def get_booking(booking_id):
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Bookings')
            return table.get_item(Key={'booking_id': booking_id}).get('Item', None)
        except Exception as e:
            logger.error(f"DynamoDB get_booking error: {e}")
            return None
    else:
        return MOCK_BOOKINGS.get(booking_id, None)

def update_booking_status(booking_id, status):
    if AWS_ENABLED:
        try:
            table = dynamodb.Table('Bookings')
            table.update_item(
                Key={'booking_id': booking_id},
                UpdateExpression="set #s = :s",
                ExpressionAttributeNames={'#s': 'status'},
                ExpressionAttributeValues={':s': status}
            )
        except Exception as e:
            logger.error(f"DynamoDB update_booking_status error: {e}")
            return False
    else:
        if booking_id in MOCK_BOOKINGS:
            MOCK_BOOKINGS[booking_id]['status'] = status
        else:
            return False
            
    # Send status update email notification
    booking = get_booking(booking_id)
    if booking:
        movie = get_movie(booking['movie_id'])
        movie_name = movie['movie_name'] if movie else "Unknown Movie"
        
        subject = f"Booking Status Updated - #{booking_id[:8]}"
        
        status_messages = {
            'Confirmed': f"Your booking #{booking_id} for '{movie_name}' has been CONFIRMED by the admin!\nNext step: Generating tickets.",
            'Tickets Generated': f"Your tickets are ready!\nBooking Ref: #{booking_id}\nShow: {movie_name}\nSeats: {booking['seats']}\nEnjoy your show!",
            'Completed': f"Thank you for attending!\nBooking #{booking_id} is marked as COMPLETED. We hope to host you again soon.",
            'Cancelled': f"Attention: Your booking #{booking_id} for '{movie_name}' has been CANCELLED. A refund will be initiated if applicable."
        }
        
        msg = status_messages.get(status, f"Your booking #{booking_id} has been transitioned to: {status}")
        publish_notification(subject, msg)
        
    return True

# ==========================================
# SNS NOTIFICATION SERVICES
# ==========================================

def subscribe_email(email):
    if not AWS_ENABLED or not SNS_TOPIC_ARN:
        logger.info(f"[Sandbox Subscription] Request sent to email: {email}")
        return
    try:
        sns_client.subscribe(
            TopicArn=SNS_TOPIC_ARN,
            Protocol='email',
            Endpoint=email
        )
        logger.info(f"Subscribed {email} to AWS SNS Topic.")
    except Exception as e:
        logger.error(f"SNS subscribe error for {email}: {e}")

def publish_notification(subject, message):
    if not AWS_ENABLED or not SNS_TOPIC_ARN:
        logger.info(f"[Sandbox Notification] Notification Triggered:\nSubject: {subject}\nMessage: {message}\n")
        return
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        logger.info(f"Successfully published notification: {subject}")
    except Exception as e:
        logger.error(f"SNS publish error: {e}")

# ==========================================
# ADMIN CREDENTIALS
# ==========================================
ADMIN_EMAIL = "admin@moviesystem.com"
ADMIN_PASSWORD_HASH = generate_password_hash("admin123")

# ==========================================
# FLASK ROUTE CONTEXT HELPERS
# ==========================================

@app.context_processor
def inject_global_vars():
    """Injects helpful variables like AWS status to all templates."""
    return {
        'aws_enabled': AWS_ENABLED,
        'session_user_name': session.get('user_name', None),
        'session_user_email': session.get('email', None),
        'is_admin': session.get('is_admin', False)
    }

# ==========================================
# FLASK ROUTES
# ==========================================

# 1. Landing Page
@app.route('/')
def index():
    movies_list = get_movies()
    featured_movies = movies_list[:3] if movies_list else []
    return render_template('index.html', featured_movies=featured_movies)

# 2. Registration Page
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        name = request.form.get('name')
        email = request.form.get('email')
        password = request.form.get('password')
        
        if not name or not email or not password:
            flash("All fields are strictly required!", "danger")
            return redirect(url_for('register'))
            
        existing_user = get_user_by_email(email)
        if existing_user or email.lower() == ADMIN_EMAIL.lower():
            flash("A user with this email already exists!", "warning")
            return redirect(url_for('register'))
            
        hashed_pw = generate_password_hash(password)
        user = create_user(name, email, hashed_pw)
        
        if user:
            flash("Registration Successful! An AWS subscription confirmation email has been sent. Please confirm it in your inbox.", "success")
            return redirect(url_for('login'))
        else:
            flash("Internal database error occurred. Please try again.", "danger")
            return redirect(url_for('register'))
            
    return render_template('register.html')

# 3. Login Page
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        
        if not email or not password:
            flash("Please enter both email and password.", "warning")
            return redirect(url_for('login'))
            
        # Admin Login Check
        if email.lower() == ADMIN_EMAIL.lower():
            if check_password_hash(ADMIN_PASSWORD_HASH, password):
                session.clear()
                session['user_id'] = 'ADMIN'
                session['user_name'] = 'System Administrator'
                session['email'] = ADMIN_EMAIL
                session['is_admin'] = True
                flash("Admin Panel unlocked successfully!", "success")
                return redirect(url_for('admin'))
            else:
                flash("Incorrect password for admin account.", "danger")
                return redirect(url_for('login'))
                
        # Regular User Login Check
        user = get_user_by_email(email)
        if user and check_password_hash(user['password'], password):
            session.clear()
            session['user_id'] = user['user_id']
            session['user_name'] = user['name']
            session['email'] = user['email']
            session['is_admin'] = False
            flash(f"Welcome back, {user['name']}!", "success")
            return redirect(url_for('movies'))
        else:
            flash("Invalid email or password combination.", "danger")
            return redirect(url_for('login'))
            
    return render_template('login.html')

# 4. Logout Route
@app.route('/logout')
def logout():
    session.clear()
    flash("Successfully signed out.", "info")
    return redirect(url_for('index'))

# 5. Movies Page
@app.route('/movies')
def movies():
    if 'user_id' not in session:
        flash("Please log in to view and book movies.", "warning")
        return redirect(url_for('login'))
    
    movies_list = get_movies()
    return render_template('movies.html', movies=movies_list)

# 6. Booking Page (Buy tickets for a specific movie_id)
@app.route('/book/<movie_id>', methods=['GET', 'POST'])
def book(movie_id):
    if 'user_id' not in session:
        flash("Authorization is required to book a ticket.", "warning")
        return redirect(url_for('login'))
        
    movie = get_movie(movie_id)
    if not movie:
        flash("Movie not found or expired.", "danger")
        return redirect(url_for('movies'))
        
    if request.method == 'POST':
        seats = request.form.get('seats')
        
        if not seats or not seats.isdigit() or int(seats) < 1:
            flash("Please enter a valid seat count (minimum 1).", "warning")
            return redirect(url_for('book', movie_id=movie_id))
            
        seats_int = int(seats)
        total_price = seats_int * int(movie['ticket_price'])
        
        booking = create_booking(session['user_id'], movie_id, seats_int, total_price)
        if booking:
            flash("Booking placed successfully! You will receive email notifications as the admin reviews it.", "success")
            return redirect(url_for('bookings'))
        else:
            flash("Could not complete booking due to a database exception.", "danger")
            return redirect(url_for('book', movie_id=movie_id))
            
    return render_template('booking.html', movie=movie)

# 7. User Booking History Page
@app.route('/bookings')
def bookings():
    if 'user_id' not in session:
        flash("Please log in to view your bookings.", "warning")
        return redirect(url_for('login'))
        
    if session.get('is_admin', False):
        return redirect(url_for('admin'))
        
    user_bookings = get_bookings_by_user(session['user_id'])
    
    # Enrich booking list with movie data for display
    enriched_bookings = []
    for b in user_bookings:
        movie = get_movie(b['movie_id'])
        enriched_bookings.append({
            'booking_id': b['booking_id'],
            'seats': b['seats'],
            'total_price': b['total_price'],
            'status': b['status'],
            'movie_name': movie['movie_name'] if movie else 'Expired Title',
            'theater': movie['theater'] if movie else 'N/A',
            'timing': movie['timing'] if movie else 'N/A'
        })
        
    return render_template('bookings.html', bookings=enriched_bookings)

# 8. Admin Control Hub Route
@app.route('/admin')
def admin():
    if 'user_id' not in session or not session.get('is_admin', False):
        flash("Restricted Access: Administrators only.", "danger")
        return redirect(url_for('index'))
        
    # Get all database records
    movies_list = get_movies()
    bookings_list = get_all_bookings()
    
    # Calculate Dashboard Analytics
    total_sales = sum(b['total_price'] for b in bookings_list if b['status'] != 'Cancelled')
    total_tickets = sum(b['seats'] for b in bookings_list if b['status'] != 'Cancelled')
    active_bookings = len([b for b in bookings_list if b['status'] in ['Requested', 'Confirmed', 'Tickets Generated']])
    total_movies = len(movies_list)
    
    # Enrich bookings for administration panel view
    enriched_bookings = []
    for b in bookings_list:
        movie = get_movie(b['movie_id'])
        # Also need a way to display user name/email. We could query Users table or handle gracefully
        # For performance/reliability, let's look up or set placeholder
        user_name = "System User"
        if AWS_ENABLED:
            try:
                users_table = dynamodb.Table('Users')
                user_res = users_table.get_item(Key={'user_id': b['user_id']})
                if user_res.get('Item'):
                    user_name = user_res['Item']['name']
            except Exception:
                pass
        else:
            u = MOCK_USERS.get(b['user_id'])
            if u:
                user_name = u['name']
                
        enriched_bookings.append({
            'booking_id': b['booking_id'],
            'user_id': b['user_id'],
            'user_name': user_name,
            'seats': b['seats'],
            'total_price': b['total_price'],
            'status': b['status'],
            'movie_name': movie['movie_name'] if movie else 'Expired Movie'
        })
        
    return render_template('admin.html', 
                           movies=movies_list, 
                           bookings=enriched_bookings,
                           sales=total_sales, 
                           tickets=total_tickets, 
                           active=active_bookings, 
                           movie_count=total_movies)

# 9. Admin Movie Management - Add Movie
@app.route('/add_movie', methods=['POST'])
def add_movie():
    if 'user_id' not in session or not session.get('is_admin', False):
        flash("Unauthorized action.", "danger")
        return redirect(url_for('index'))
        
    name = request.form.get('movie_name')
    theater = request.form.get('theater')
    timing = request.form.get('timing')
    price = request.form.get('ticket_price')
    image_url = request.form.get('image_url')
    
    if not name or not theater or not timing or not price:
        flash("All movie details are required.", "warning")
        return redirect(url_for('admin'))
        
    movie = create_movie(name, theater, timing, price, image_url)
    if movie:
        flash("New movie created successfully!", "success")
    else:
        flash("Failed to register movie in database.", "danger")
        
    return redirect(url_for('admin'))

# 10. Admin Movie Management - Edit Movie
@app.route('/edit_movie/<movie_id>', methods=['POST'])
def edit_movie(movie_id):
    if 'user_id' not in session or not session.get('is_admin', False):
        flash("Unauthorized action.", "danger")
        return redirect(url_for('index'))
        
    name = request.form.get('movie_name')
    theater = request.form.get('theater')
    timing = request.form.get('timing')
    price = request.form.get('ticket_price')
    image_url = request.form.get('image_url')
    
    if not name or not theater or not timing or not price:
        flash("All fields are required to update a movie.", "warning")
        return redirect(url_for('admin'))
        
    success = update_movie(movie_id, name, theater, timing, price, image_url)
    if success:
        flash("Movie specifications updated successfully!", "success")
    else:
        flash("Failed to update movie.", "danger")
        
    return redirect(url_for('admin'))

# 11. Admin Movie Management - Delete Movie
@app.route('/delete_movie/<movie_id>')
def delete_movie_route(movie_id):
    if 'user_id' not in session or not session.get('is_admin', False):
        flash("Unauthorized action.", "danger")
        return redirect(url_for('index'))
        
    success = delete_movie(movie_id)
    if success:
        flash("Movie deleted successfully.", "info")
    else:
        flash("Unable to delete movie from database.", "danger")
        
    return redirect(url_for('admin'))

# 12. Admin Booking Management - Update Booking Status
@app.route('/update_status/<booking_id>', methods=['POST'])
def update_status(booking_id):
    if 'user_id' not in session or not session.get('is_admin', False):
        flash("Unauthorized action.", "danger")
        return redirect(url_for('index'))
        
    new_status = request.form.get('status')
    valid_statuses = ['Requested', 'Confirmed', 'Tickets Generated', 'Completed', 'Cancelled']
    
    if new_status not in valid_statuses:
        flash("Invalid lifecycle status.", "warning")
        return redirect(url_for('admin'))
        
    success = update_booking_status(booking_id, new_status)
    if success:
        flash(f"Booking status advanced to '{new_status}' successfully!", "success")
    else:
        flash("Could not update the booking status.", "danger")
        
    return redirect(url_for('admin'))

# ==========================================
# ERROR HANDLERS
# ==========================================

@app.errorhandler(404)
def page_not_found(e):
    return render_template('index.html'), 404

# ==========================================
# SERVER INITIATION
# ==========================================

if __name__ == '__main__':
    # Flask defaults to port 5000; EC2 configuration often uses 5000 or maps to port 80
    host_addr = '0.0.0.0'
    port_num = 5000
    logger.info(f"Booting up system on {host_addr}:{port_num}...")
    app.run(host=host_addr, port=port_num, debug=True)
