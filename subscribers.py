import os
import pywhatkit as kit
import time
from dotenv import load_dotenv
from datetime import datetime
import logging as lgn
from pathlib import Path

# setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"whatsapp_sender.log"

lgn.basicConfig(
    level=lgn.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        lgn.FileHandler(log_file, mode='a'),  # 'a' for append mode
        lgn.StreamHandler()  # Also print to console
    ]
)

logger = lgn.getLogger(__name__)

# load environment variables
load_dotenv()

# get environment varaiables from .env
google_form = os.getenv("GOOGLE_LINK")
recepients = os.getenv("CONTACTS")
message_template = os.getenv("MESSAGE_TEMPLATE")
sender_name = os.getenv("SENDER_NAME")

# create a list with the recipients numbers
contact_list = [num.strip() for num in recepients.split(',')]

# create contact mask
def mask_phone(phone_number):
    if len(phone_number) > 4:
        return f"***{phone_number[-4:]}"
    return "****"

# create the message
def create_message():
    message = message_template.replace('{form_link}', google_form)
    message = message.replace('{sender}', sender_name)
    return message

# send message to contacts on my list
def send_messages():
    message=create_message()

    logger.info(f"=== WhatsApp Sender Started ===")
    logger.info(f"Total contacts to message: {len(contact_list)}")
    logger.info(f"Sender: {sender_name}")
    logger.info(f"Message template prepared (form link included)")
    
    success_count = 0
    failed_count = 0

    for idx, phone_no in enumerate(contact_list, 1):
        masked_number = mask_phone(phone_no)
        
        try:
            logger.info(f"[{idx}/{len(contact_list)}] Processing contact ***{masked_number}...")

            # send message instantly; wait  15 seconds before closing tab
            kit.sendwhatmsg_instantly(phone_no, message, 50, False)
            logger.info(f"✓ Message sent successfully to ***{masked_number}")
            success_count += 1

            # time between each sent mails to a
            # void whatsapp restriction
            if idx < len(contact_list):
                
                logger.info("Waiting 60 seconds" \
                            " before next message...")
                time.sleep(60)
                
                
        except Exception as e:
            
            logger.error(f"✗ Failed to send to ***{masked_number}: {str(e)}")
            failed_count += 1
            continue


    logger.info(f"\n=== Summary ===")
    logger.info(f"Total sent: {success_count}")
    logger.info(f"Total failed: {failed_count}")
    logger.info(f"Log saved to: {log_file}")
    logger.info(f"=== Process Complete ===")

if __name__ == "__main__":
    # validate that all required environment variables are set
    if not all([google_form, recepients, message_template, sender_name]):
        logger.error("Missing required environment variables in .env file")
        logger.error("Required: google_form, message_template, sender_name, recepients")
    else:
        try:
            send_messages()
        except KeyboardInterrupt:
            logger.warning("\nProcess interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")