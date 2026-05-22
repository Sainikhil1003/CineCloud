"""
Movie Ticket Booking System using Flask and AWS
Updated for DynamoDB tables using EMAIL as Partition Key
Users table name: Userss
"""

import os
import uuid
import logging
from flask import Flask, render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash, check_password_hash
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError, NoCredentialsError

# ==========================================
# LOGGING
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# ==========================================
# FLASK APP
# ==========================================

app = Flask(__name__)

app.secret_key = os.environ.get(
    'FLASK_SECRET_KEY',
    'cyberpunk_secret_key_102938'
)

# ==========================================
# AWS CONFIGURATION
# ==========================================

AWS_ENABLED = True
SNS_TOPIC_ARN = None

MOCK_USERS = {}
MOCK_MOVIES = {}
MOCK_BOOKINGS = {}

try:
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-south-1')

    dynamodb = boto3.resource(
        'dynamodb',
        region_name=aws_region
    )

    sns_client = boto3.client(
        'sns',
        region_name=aws_region
    )

    sts_client = boto3.client(
        'sts',
        region_name=aws_region
    )

    sts_client.get_caller_identity()

    logger.info("AWS Credentials verified successfully.")

except (NoCredentialsError, ClientError, Exception) as e:
    AWS_ENABLED = False
    logger.warning(f"AWS initialization failed: {e}")

# ==========================================
# DEFAULT MOVIES
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
    }
]

# ==========================================
# INITIALIZE AWS RESOURCES
# ==========================================

def initialize_aws_resources():

    global SNS_TOPIC_ARN

    if not AWS_ENABLED:
        logger.info("Running in MOCK MODE")
        return

    try:

        existing_tables = [
            table.name for table in dynamodb.tables.all()
        ]

        def create_table(name, pk):

            if name not in existing_tables:

                logger.info(f"Creating table {name}")

                table = dynamodb.create_table(
                    TableName=name,
                    KeySchema=[
                        {
                            'AttributeName': pk,
                            'KeyType': 'HASH'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': pk,
                            'AttributeType': 'S'
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )

                table.wait_until_exists()

        create_table('Userss', 'email')
        create_table('Movies', 'email')
        create_table('Bookings', 'email')

        # Seed Movies
        movies_table = dynamodb.Table('Movies')

        existing = movies_table.scan(Limit=1)

        if not existing.get('Items'):

            for mov in DEFAULT_MOVIES:

                movie_id = str(uuid.uuid4())

                movies_table.put_item(
                    Item={
                        'email': f"{movie_id}@movie.com",
                        'movie_id': movie_id,
                        'movie_name': mov['movie_name'],
                        'theater': mov['theater'],
                        'timing': mov['timing'],
                        'ticket_price': int(mov['ticket_price']),
                        'image_url': mov['image_url']
                    }
                )

        # SNS Topic
        topic_resp = sns_client.create_topic(
            Name='cinesns'
        )

        SNS_TOPIC_ARN = topic_resp['TopicArn']

    except Exception as e:
        logger.error(f"AWS Resource Initialization Error: {e}")

initialize_aws_resources()

# ==========================================
# USER FUNCTIONS
# ==========================================

def get_user_by_email(email):

    if AWS_ENABLED:

        try:

            table = dynamodb.Table('Userss')

            response = table.get_item(
                Key={'email': email}
            )

            return response.get('Item', None)

        except Exception as e:
            logger.error(f"get_user_by_email error: {e}")
            return None

    return None


def create_user(name, email, password_hash):

    user_id = str(uuid.uuid4())

    user_data = {
        'email': email,
        'user_id': user_id,
        'name': name,
        'password': password_hash
    }

    if AWS_ENABLED:

        try:

            table = dynamodb.Table('Userss')

            table.put_item(
                Item=user_data
            )

        except Exception as e:
            logger.error(f"create_user error: {e}")
            return None

    subscribe_email(email)

    return user_data

# ==========================================
# MOVIE FUNCTIONS
# ==========================================

def get_movies():

    if AWS_ENABLED:

        try:

            table = dynamodb.Table('Movies')

            return table.scan().get('Items', [])

        except Exception as e:
            logger.error(f"get_movies error: {e}")

    return []


def get_movie(movie_id):

    if AWS_ENABLED:

        try:

            table = dynamodb.Table('Movies')

            response = table.scan(
                FilterExpression=Attr('movie_id').eq(movie_id)
            )

            items = response.get('Items', [])

            return items[0] if items else None

        except Exception as e:
            logger.error(f"get_movie error: {e}")

    return None


def create_movie(movie_name, theater, timing, ticket_price, image_url):

    movie_id = str(uuid.uuid4())

    movie_data = {
        'email': f"{movie_id}@movie.com",
        'movie_id': movie_id,
        'movie_name': movie_name,
        'theater': theater,
        'timing': timing,
        'ticket_price': int(ticket_price),
        'image_url': image_url
    }

    if AWS_ENABLED:

        try:

            table = dynamodb.Table('Movies')

            table.put_item(
                Item=movie_data
            )

        except Exception as e:
            logger.error(f"create_movie error: {e}")
            return None

    return movie_data


def update_movie(movie_id, movie_name, theater, timing, ticket_price, image_url):

    if AWS_ENABLED:

        try:

            movie = get_movie(movie_id)

            if not movie:
                return False

            table = dynamodb.Table('Movies')

            table.update_item(
                Key={
                    'email': movie['email']
                },
                UpdateExpression="""
                set movie_name=:n,
                theater=:th,
                timing=:ti,
                ticket_price=:p,
                image_url=:u
                """,
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
            logger.error(f"update_movie error: {e}")

    return False


def delete_movie(movie_id):

    if AWS_ENABLED:

        try:

            movie = get_movie(movie_id)

            if not movie:
                return False

            table = dynamodb.Table('Movies')

            table.delete_item(
                Key={
                    'email': movie['email']
                }
            )

            return True

        except Exception as e:
            logger.error(f"delete_movie error: {e}")

    return False

# ==========================================
# BOOKING FUNCTIONS
# ==========================================

def create_booking(user_id, movie_id, seats, total_price):

    booking_id = str(uuid.uuid4())

    booking_data = {
        'email': f"{booking_id}@booking.com",
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

            table.put_item(
                Item=booking_data
            )

        except Exception as e:
            logger.error(f"create_booking error: {e}")
            return None

    movie = get_movie(movie_id)

    movie_name = movie['movie_name'] if movie else "Unknown Movie"

    publish_notification(
        f"Booking Created - {booking_id[:8]}",
        f"Movie: {movie_name}\nSeats: {seats}"
    )

    return booking_data


def get_booking(booking_id):

    if AWS_ENABLED:

        try:

            table = dynamodb.Table('Bookings')

            response = table.scan(
                FilterExpression=Attr('booking_id').eq(booking_id)
            )

            items = response.get('Items', [])

            return items[0] if items else None

        except Exception as e:
            logger.error(f"get_booking error: {e}")

    return None


def get_all_bookings():

    if AWS_ENABLED:

        try:

            table = dynamodb.Table('Bookings')

            return table.scan().get('Items', [])

        except Exception as e:
            logger.error(f"get_all_bookings error: {e}")

    return []


def get_bookings_by_user(user_id):

    if AWS_ENABLED:

        try:

            table = dynamodb.Table('Bookings')

            response = table.scan(
                FilterExpression=Attr('user_id').eq(user_id)
            )

            return response.get('Items', [])

        except Exception as e:
            logger.error(f"get_bookings_by_user error: {e}")

    return []


def update_booking_status(booking_id, status):

    if AWS_ENABLED:

        try:

            booking = get_booking(booking_id)

            if not booking:
                return False

            table = dynamodb.Table('Bookings')

            table.update_item(
                Key={
                    'email': booking['email']
                },
                UpdateExpression="set #s=:s",
                ExpressionAttributeNames={
                    '#s': 'status'
                },
                ExpressionAttributeValues={
                    ':s': status
                }
            )

            return True

        except Exception as e:
            logger.error(f"update_booking_status error: {e}")

    return False

# ==========================================
# SNS FUNCTIONS
# ==========================================

def subscribe_email(email):

    if not AWS_ENABLED or not SNS_TOPIC_ARN:
        return

    try:

        sns_client.subscribe(
            TopicArn=SNS_TOPIC_ARN,
            Protocol='email',
            Endpoint=email
        )

    except Exception as e:
        logger.error(f"subscribe_email error: {e}")


def publish_notification(subject, message):

    if not AWS_ENABLED or not SNS_TOPIC_ARN:
        return

    try:

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )

    except Exception as e:
        logger.error(f"publish_notification error: {e}")

# ==========================================
# ADMIN
# ==========================================

ADMIN_EMAIL = "admin@moviesystem.com"

ADMIN_PASSWORD_HASH = generate_password_hash(
    "admin123"
)

# ==========================================
# ROUTES
# ==========================================

@app.route('/')
def index():

    movies = get_movies()

    return render_template(
        'index.html',
        featured_movies=movies[:3]
    )


@app.route('/register', methods=['GET', 'POST'])
def register():

    if request.method == 'POST':

        name = request.form.get('name')
        email = request.form.get('email')
        password = request.form.get('password')

        existing = get_user_by_email(email)

        if existing:
            flash("User already exists")
            return redirect(url_for('register'))

        hashed_pw = generate_password_hash(password)

        user = create_user(
            name,
            email,
            hashed_pw
        )

        if user:
            flash("Registration successful")
            return redirect(url_for('login'))

    return render_template('register.html')


@app.route('/login', methods=['GET', 'POST'])
def login():

    if request.method == 'POST':

        email = request.form.get('email')
        password = request.form.get('password')

        # Admin Login
        if email == ADMIN_EMAIL:

            if check_password_hash(
                ADMIN_PASSWORD_HASH,
                password
            ):

                session['user_id'] = 'ADMIN'
                session['is_admin'] = True

                return redirect(url_for('admin'))

        # User Login
        user = get_user_by_email(email)

        if user and check_password_hash(
            user['password'],
            password
        ):

            session['user_id'] = user['user_id']
            session['email'] = user['email']
            session['user_name'] = user['name']

            return redirect(url_for('movies'))

        flash("Invalid Credentials")

    return render_template('login.html')


@app.route('/movies')
def movies():

    movies = get_movies()

    return render_template(
        'movies.html',
        movies=movies
    )


@app.route('/book/<movie_id>', methods=['GET', 'POST'])
def book(movie_id):

    movie = get_movie(movie_id)

    if request.method == 'POST':

        seats = int(request.form.get('seats'))

        total_price = seats * int(movie['ticket_price'])

        create_booking(
            session['user_id'],
            movie_id,
            seats,
            total_price
        )

        flash("Booking Successful")

        return redirect(url_for('movies'))

    return render_template(
        'booking.html',
        movie=movie
    )


@app.route('/admin')
def admin():

    if not session.get('is_admin'):
        return redirect(url_for('login'))

    movies = get_movies()

    bookings = get_all_bookings()

    return render_template(
        'admin.html',
        movies=movies,
        bookings=bookings
    )


@app.route('/logout')
def logout():

    session.clear()

    return redirect(url_for('index'))

# ==========================================
# RUN APP
# ==========================================

if __name__ == '__main__':

    logger.info("Starting Flask Server")

    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )
