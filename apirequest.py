import clickhouse_connect
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import logging as lgn
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import os


# Configure logging
log_dir = Path("logs")
log_file = log_dir / f"quote_emailer.log"

lgn.basicConfig(
    level=lgn.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        lgn.FileHandler(log_file, mode='a'),
        lgn.StreamHandler()
    ]
)
logger = lgn.getLogger(__name__)

# Configuration
load_dotenv()

# ClickHouse Configuration
CH_HOST =  os.getenv("CLICK_HOST")
CH_PORT = os.getenv("CLICK_PORT")
CH_USERNAME = os.getenv("CLICK_USER")
CH_PASSWORD = os.getenv("CLICK_PASSWORD")
CH_DATABASE = os.getenv("CLICK_DATABASE")

# Gmail Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")

def get_subscribers():
    """Extract first names, last names, and emails from active subscribers"""
    logger.info("=" * 50)
    logger.info("STARTING: Fetching active subscribers from ClickHouse")
    logger.info(f"ClickHouse Host: {CH_HOST}:{CH_PORT}")
    logger.info(f"Database: {CH_DATABASE}")
    
    try:
        start_time = time.time()
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USERNAME,
            password=CH_PASSWORD,
            database=CH_DATABASE
        )
        connection_time = time.time() - start_time
        logger.info(f"ClickHouse connection established in {connection_time:.2f}s")
        
        # Query active subscribers
        query = """
            WITH subscribers AS(
            SELECT "First Name." AS first_name, "Last Name." AS last_name,
	        "Email." AS email, "Would love to get frequent motivational quotes?" AS status
            FROM quote_suscribers
            )
            SELECT first_name, last_name, email
            FROM subscribers
            WHERE email IS NOT NULL 
            AND status  = 'Yes'
            """
        logger.info(f"Executing query: {query.strip()}")
        
        query_start = time.time()
        result = client.query(query)
        subscribers = result.result_rows
        query_time = time.time() - query_start
        
        logger.info(f"Query executed in {query_time:.2f}s")
        logger.info(f"SUCCESS: Retrieved {len(subscribers)} active subscribers")
        
        if len(subscribers) == 0:
            logger.warning("No active subscribers found in database")
        
        return subscribers
        
    except Exception as e:
        logger.error(f"FAILED: ClickHouse connection/query error: {str(e)}", exc_info=True)
        return []

def get_random_quote():
    """Fetch a random quote from ZenQuotes API"""
    logger.info("Fetching random quote from ZenQuotes API")
    
    try:
        start_time = time.time()
        response = requests.get('https://zenquotes.io/api/random', timeout=30)
        api_time = time.time() - start_time
        
        logger.info(f"ZenQuotes API response status: {response.status_code} (took {api_time:.2f}s)")
        
        if response.status_code == 200:
            data = response.json()
            quote = data[0]['q']
            author = data[0]['a']
            full_quote = f'"{quote}" - {author}'
            logger.info(f"Quote retrieved: {full_quote[:100]}...")
            return full_quote
        else:
            logger.warning(f"ZenQuotes API returned non-200 status: {response.status_code}")
            fallback_quote = "Believe you can and you're halfway there. - Theodore Roosevelt"
            logger.info(f"Using fallback quote: {fallback_quote}")
            return fallback_quote
            
    except requests.exceptions.Timeout:
        logger.error("ZenQuotes API request timed out after 30 seconds")
        return "Believe you can and you're halfway there. - Theodore Roosevelt"
    except Exception as e:
        logger.error(f"Error fetching quote from ZenQuotes: {str(e)}", exc_info=True)
        return "Believe you can and you're halfway there. - Theodore Roosevelt"

def send_email(recipient_email, first_name, last_name, quote):
    """Send email with quote to subscriber"""
    logger.info(f"Preparing email for: {first_name} {last_name} <{recipient_email}>")
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = SENDER_EMAIL
        msg['To'] = recipient_email
        msg['Subject'] = 'Your Daily Inspiration Quote'
        
        # Email body
        html = f"""
        <html>
          <body>
            <h3>Hello {first_name} {last_name}!</h3>
            <p>Here's your inspirational quote for today:</p>
            <blockquote style="font-style: italic; font-size: 18px; margin: 20px;">
              {quote}
            </blockquote>
            <p>Until tomorrow, stay inspired!</p>
            <br>
            <p>Team MindFuel</p>
          </body>
        </html>
        """
        
        text = f"""
        Hello {first_name} {last_name}!
        
        Here's your inspirational quote for today:
        
        {quote}
        
        Until tomorrow, stay inspired!

        Team Mindfuel
        """
        
        # Attach both plain text and HTML versions
        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(html, 'html')
        msg.attach(part1)
        msg.attach(part2)
        
        logger.info(f"Connecting to SMTP server: {SMTP_SERVER}:{SMTP_PORT}")
        
        # Send email
        start_time = time.time()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
            server.starttls()
            logger.debug("SMTP TLS connection established")
            
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            logger.debug("SMTP authentication successful")
            
            server.send_message(msg)
            send_time = time.time() - start_time
            
        logger.info(f"SUCCESS: Email sent to {recipient_email} in {send_time:.2f}s")
        return True
        
    except smtplib.SMTPAuthenticationError:
        logger.error(f"FAILED: SMTP authentication failed for {recipient_email}. Check email/password.")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"FAILED: SMTP error sending to {recipient_email}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"FAILED: Unexpected error sending email to {recipient_email}: {str(e)}", exc_info=True)
        return False

def main():
    """Main function to orchestrate the email sending process"""
    logger.info("=" * 70)
    logger.info("QUOTE EMAILER PIPELINE STARTED")
    logger.info(f"Execution started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    pipeline_start = time.time()
    
    # Get all active subscribers
    subscribers = get_subscribers()
    
    if not subscribers:
        logger.error("PIPELINE ABORTED: No active subscribers found")
        return
    
    logger.info("=" * 50)
    logger.info("STARTING: Email sending process")
    logger.info(f"Total subscribers to process: {len(subscribers)}")
    logger.info("=" * 50)
    
    # Send email to each subscriber with a random quote
    success_count = 0
    failed_count = 0
    
    for idx, (first_name, last_name, email) in enumerate(subscribers, 1):
        logger.info("-" * 50)
        logger.info(f"Processing subscriber {idx}/{len(subscribers)}")
        
        # Fetch a new random quote for each subscriber
        quote = get_random_quote()
        
        # Send email
        if send_email(email, first_name, last_name, quote):
            success_count += 1
        
        # Rate limiting
        if idx < len(subscribers):
            logger.info("Waiting 30 seconds before next email...")
            time.sleep(30)
    
    # Pipeline summary
    pipeline_time = time.time() - pipeline_start
    
    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETED")
    logger.info("=" * 70)
    logger.info(f"Total execution time: {pipeline_time:.2f}s")
    logger.info(f"Total active subscribers: {len(subscribers)}")
    logger.info(f"Emails sent successfully: {success_count}")
    logger.info(f"Emails failed: {failed_count}")
    logger.info(f"Success rate: {(success_count/len(subscribers)*100):.1f}%")
    logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user (Ctrl+C)")
    except Exception as e:
        logger.critical(f"CRITICAL ERROR: Pipeline failed with exception: {str(e)}", exc_info=True)