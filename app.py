import os
import logging
from flask import Flask, jsonify, request as flask_request, abort as flask_abort
from flask_cors import CORS
from dotenv import load_dotenv
import time
import random
import re
import hmac
import hashlib
import telebot
from telebot import types
from urllib.parse import unquote, parse_qs
from datetime import datetime as dt, timezone, timedelta
import json
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, UniqueConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from curl_cffi.requests import AsyncSession, RequestsError
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad
from pytoniq import LiteBalancer
import asyncio
import math
import secrets # Add this import for generating secure random strings
import uuid


load_dotenv()

# --- Configuration Constants ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24 # 24 hours for Telegram Mini App auth data
TONNEL_SENDER_INIT_DATA = os.environ.get("TONNEL_SENDER_INIT_DATA")
TONNEL_GIFT_SECRET = os.environ.get("TONNEL_GIFT_SECRET", "yowtfisthispieceofshitiiit")
ADMIN_USER_IDS = [6529588448, 5146625949]
TARGET_WITHDRAWER_ID = os.environ.get("TARGET_WITHDRAWER_ID") # Add this line

DEPOSIT_RECIPIENT_ADDRESS_RAW = os.environ.get("DEPOSIT_WALLET_ADDRESS")
DEPOSIT_COMMENT = os.environ.get("DEPOSIT_COMMENT", "e8a1vds9yal")
PENDING_DEPOSIT_EXPIRY_MINUTES = 30

BIG_WIN_CHANNEL_ID = -1002786435659  # The channel ID you provided
BOT_USERNAME_FOR_LINK = "Hunter_Case_bot" # Your bot's username for the link

UPGRADE_MAX_CHANCE = Decimal('75.0')  # Maximum possible chance in %
UPGRADE_MIN_CHANCE = Decimal('3.0')   # Minimum possible chance in %
# RiskFactor: lower value means chance drops faster for higher multipliers (X)
# e.g., 0.60 means for X=2, chance is MaxChance*0.6; for X=3, chance is MaxChance*0.6*0.6
UPGRADE_RISK_FACTOR = Decimal('0.60')
UPGRADE_HOUSE_EDGE_FACTOR = Decimal('0.80')

RTP_TARGET = Decimal('0.65') # 85% Return to Player target for all cases and slots
TON_TO_STARS_RATE_BACKEND = 250
PAYMENT_PROVIDER_TOKEN = "" # Add this to your .env file!

SPECIAL_REFERRAL_RATES = {
    "SpinXD": Decimal('0.20')  # Username (case-insensitive) and their 20% rate
}
DEFAULT_REFERRAL_RATE = Decimal('0.10') # The standard 10% rate for everyone else

# New Emoji Gift Definitions
EMOJI_GIFTS_BACKEND = {
    "Heart":  {"id": "5170145012310081615", "value": 15},
    "Bear":   {"id": "5170233102089322756", "value": 15},
    "Rose":   {"id": "5168103777563050263", "value": 25},
    "Rocket": {"id": "5170564780938756245", "value": 50},
    "Bottle": {"id": "6028601630662853006", "value": 50}
}
KISS_FROG_MODEL_STATIC_PERCENTAGES = {
    "Brewtoad": 0.5,
    "Zodiak Croak": 0.5,
    "Rocky Hopper": 0.5,
    "Puddles": 0.5,
    "Lucifrog": 0.5,
    "Honeyhop": 0.5,
    "Count Croakula": 0.5,
    "Lilie Pond": 0.5,
    "Frogmaid": 0.5,
    "Happy Pepe": 0.5,
    "Melty Butter": 0.5,
    "Sweet Dream": 0.5,
    "Tree Frog": 0.5,
    "Lava Leap": 1.0,
    "Tesla Frog": 1.0,
    "Trixie": 1.0,
    "Pond Fairy": 1.0,
    "Icefrog": 1.0,
    "Hopberry": 1.5,
    "Boingo": 1.5,
    "Prince Ribbit": 1.5,
    "Toadstool": 1.5,
    "Cupid": 1.5,
    "Ms. Toad": 1.5,
    "Desert Frog": 1.5,
    "Silver": 2.0,
    "Bronze": 2.0,
    "Poison": 2.5,
    "Ramune": 2.5,
    "Lemon Drop": 2.5,
    "Minty Bloom": 2.5,
    "Void Hopper": 2.5,
    "Sarutoad": 2.5,
    "Duskhopper": 2.5,
    "Starry Night": 2.5,
    "Ectofrog": 2.5,
    "Ectobloom": 2.5,
    "Melon": 3.0,
    "Banana Pox": 3.0,
    "Frogtart": 3.0,
    "Sea Breeze": 4.0,
    "Sky Leaper": 4.0,
    "Toadberry": 4.0,
    "Peach": 4.0,
    "Lily Pond": 4.0,
    "Frogwave": 4.0,
    "Cranberry": 4.0,
    "Lemon Juice": 4.0,
    "Tide Pod": 4.0,
    "Brownie": 4.0,
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backend_app.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

EMOJI_GIFT_IMAGES = {
    "Heart": "https://github.com/Vasiliy-katsyka/gifthunter/blob/main/gifts_emoji_by_gifts_changes_bot_AgADYEwAAiHMUUk.png?raw=true",
    "Bear": "https://github.com/Vasiliy-katsyka/gifthunter/blob/main/gifts_emoji_by_gifts_changes_bot_AgADomAAAvRzSEk.png?raw=true",
    "Rose": "https://github.com/Vasiliy-katsyka/gifthunter/blob/main/gifts_emoji_by_gifts_changes_bot_AgADslsAAqCxSUk.png?raw=true",
    "Rocket": "https://github.com/Vasiliy-katsyka/gifthunter/blob/main/gifts_emoji_by_gifts_changes_bot_AgAD9lAAAsBFUUk.png?raw=true",
    "Bottle": "https://github.com/Vasiliy-katsyka/gifthunter/blob/main/gifts_emoji_by_gifts_changes_bot_AgADA2cAAm0PqUs.png?raw=true"
}

# Basic checks for essential environment variables
if not BOT_TOKEN:
    logger.error("BOT_TOKEN not set for backend (needed for initData validation)!")
if not DATABASE_URL:
    logger.error("DATABASE_URL not set!")
    exit("DATABASE_URL is not set. Exiting.")
if not TONNEL_SENDER_INIT_DATA:
    logger.warning("TONNEL_SENDER_INIT_DATA not set! Tonnel gift withdrawal will likely fail.")

NORMAL_WEBAPP_URL = "https://vasiliy-katsyka.github.io/gifthunter"
MAINTENANCE_WEBAPP_URL = "https://vasiliy-katsyka.github.io/maintencaincec" # If you still use this
# Example: Choose based on an environment variable or a fixed value for production
WEBAPP_URL = NORMAL_WEBAPP_URL # Assuming normal operation on the server

API_BASE_URL = "https://gifthunter.onrender.com" # Your backend API URL

DEFAULT_REFERRAL_RATE = Decimal('0.10')

# --- START OF NEW CODE ---
# Define users with boosted luck and the multiplier for their valuable prize chances
BOOSTED_LUCK_USERS = {
    512257998: 2,
    5146625949: 5, # Your ID with a 5x multiplier
    8262163216: 10
}
# Define what counts as a "valuable" prize (e.g., worth more than the case price)
VALUABLE_PRIZE_THRESHOLD_MULTIPLIER = Decimal('1.0')

# NEW: This is the percentage of "common" prize probability that we will remove
# and give to the valuable prizes for boosted users. 0.5 means 50%.
BOOSTED_LUCK_REALLOCATION_FACTOR = Decimal('0.50')

# --- SQLAlchemy Database Setup ---
engine = create_engine(
    DATABASE_URL,
    pool_size=10,          # Start with 10 persistent connections
    max_overflow=20,       # Allow up to 20 more connections during a spike
    pool_recycle=300,      # Recycle connections every 5 minutes
    pool_pre_ping=True     # Check if a connection is still alive before using it
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Database Models ---
class User(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=False)
    username = Column(String, nullable=True, index=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    ton_balance = Column(Float, default=0.0, nullable=False)
    star_balance = Column(Integer, default=0, nullable=False)
    referral_code = Column(String, unique=True, index=True, nullable=True)
    referred_by_id = Column(BigInteger, ForeignKey("users.id"), nullable=True)
    referral_earnings_pending = Column(Float, default=0.0, nullable=False)
    total_won_ton = Column(Float, default=0.0, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    inventory = relationship("InventoryItem", back_populates="owner", cascade="all, delete-orphan")
    pending_deposits = relationship("PendingDeposit", back_populates="owner")
    referrer = relationship("User", remote_side=[id], foreign_keys=[referred_by_id], back_populates="referrals_made", uselist=False)
    referrals_made = relationship("User", back_populates="referrer")

class NFT(Base):
    __tablename__ = "nfts"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True, index=True, nullable=False)
    image_filename = Column(String, nullable=True)
    floor_price = Column(Float, default=0.0, nullable=False)
    __table_args__ = (UniqueConstraint('name', name='uq_nft_name'),)

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    nft_id = Column(Integer, ForeignKey("nfts.id"), nullable=True)
    item_name_override = Column(String, nullable=True)
    item_image_override = Column(String, nullable=True)
    current_value = Column(Float, nullable=False)
    upgrade_multiplier = Column(Float, default=1.0, nullable=False)
    obtained_at = Column(DateTime(timezone=True), server_default=func.now())
    variant = Column(String, nullable=True)
    is_ton_prize = Column(Boolean, default=False, nullable=False)
    owner = relationship("User", back_populates="inventory")
    nft = relationship("NFT")

class PendingDeposit(Base):
    __tablename__ = "pending_deposits"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    original_amount_ton = Column(Float, nullable=False)
    final_amount_nano_ton = Column(BigInteger, nullable=False, index=True)
    expected_comment = Column(String, nullable=False, index=True, unique=True) # Comment is now the unique ID
    status = Column(String, default="pending", index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)
    owner = relationship("User", back_populates="pending_deposits")


class PromoCode(Base):
    __tablename__ = "promo_codes"
    id = Column(Integer, primary_key=True, index=True)
    code_text = Column(String, unique=True, index=True, nullable=False)
    activations_left = Column(Integer, nullable=False, default=0)
    ton_amount = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

class UserPromoCodeRedemption(Base):
    __tablename__ = "user_promo_code_redemptions"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    promo_code_id = Column(Integer, ForeignKey("promo_codes.id", ondelete="CASCADE"), nullable=False)
    redeemed_at = Column(DateTime(timezone=True), server_default=func.now())
    user = relationship("User")
    promo_code = relationship("PromoCode")
    __table_args__ = (UniqueConstraint('user_id', 'promo_code_id', name='uq_user_promo_redemption'),)

class Deposit(Base):
    __tablename__ = "deposits"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    ton_amount = Column(Float, nullable=False)
    deposit_type = Column(String, nullable=False) # Will be 'TON' or 'STARS'
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    user = relationship("User")

class DailyStats(Base):
    __tablename__ = "daily_stats"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime(timezone=True), server_default=func.now(), unique=True)
    new_users = Column(Integer, default=0)
    cases_opened = Column(Integer, default=0)
    ton_deposited = Column(Float, default=0.0)
    ton_won = Column(Float, default=0.0)

class AllTimeStats(Base):
    __tablename__ = "all_time_stats"
    id = Column(Integer, primary_key=True)
    total_users = Column(Integer, default=0)
    total_cases_opened = Column(Integer, default=0)
    total_ton_deposited = Column(Float, default=0.0)
    total_ton_won = Column(Float, default=0.0)

# --- In the --- Database Models --- section, update the MailingListMessage class ---

class MailingListMessage(Base):
    __tablename__ = "mailing_list_messages"
    id = Column(Integer, primary_key=True, index=True)
    message_text = Column(String)
    file_id = Column(String, nullable=True)
    file_type = Column(String, nullable=True)  # 'photo', 'video', 'animation'
    button_text = Column(String, nullable=True) # ADD THIS LINE
    button_url = Column(String, nullable=True)  # ADD THIS LINE
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    sent_by = Column(BigInteger, ForeignKey("users.id"))

class MailingListUserStatus(Base):
    __tablename__ = "mailing_list_user_status"
    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey("mailing_list_messages.id"))
    user_id = Column(BigInteger, ForeignKey("users.id"))
    sent_at = Column(DateTime(timezone=True), server_default=func.now())
    __table_args__ = (UniqueConstraint('message_id', 'user_id', name='_message_user_uc'),)

# Create database tables
Base.metadata.create_all(bind=engine)

bot = telebot.TeleBot(BOT_TOKEN, threaded=False) if BOT_TOKEN else None

# --- In app.py ---

# Make sure you have this import at the top of your file with the others
import threading

# ... (keep all your existing code, models, and constants before this line) ...

if bot: # Ensure bot instance exists

    # --- Robust Bot Handlers for High-Load Environments ---

    def register_referral_in_background(user_data):
        """
        This function runs in a separate thread to avoid blocking the bot.
        It handles the API call to register a new referral.
        """
        try:
            import requests
            # The bot calls its own backend API to register the referral relationship
            response = requests.post(f"{API_BASE_URL}/api/register_referral", json=user_data, timeout=30) # Increased timeout for safety
            if response.status_code == 200:
                logger.info(f"Background referral for user {user_data['user_id']} SUCCESS. Response: {response.json()}")
            else:
                logger.error(f"Background referral for user {user_data['user_id']} FAILED. Status: {response.status_code}, Response: {response.text}")
        except Exception as e_api:
            logger.error(f"Background API call to /api/register_referral failed for user {user_data['user_id']}: {e_api}")


    # --- In app.py ---
    
    # ... (keep all your code before this function) ...
    
    # --- Find and REPLACE the entire send_welcome function ---
    @bot.message_handler(commands=['start'])
    def send_welcome(message):
        """
        Handles the /start command. It now acts as a gatekeeper, checking for
        channel subscriptions before allowing the user to open the Web App.
        """
        try:
            user_id = message.chat.id
            tg_user = message.from_user
            logger.info(f"User {user_id} ({tg_user.username or tg_user.first_name}) started the bot. Message: {message.text}")
    
            # --- START OF SUBSCRIPTION CHECK LOGIC ---
            missing_subscriptions = []
            for channel_id in REQUIRED_CHANNELS:
                try:
                    chat_member = bot.get_chat_member(channel_id, user_id)
                    if chat_member.status not in ['member', 'administrator', 'creator']:
                        missing_subscriptions.append(channel_id)
                except Exception as e:
                    logger.warning(f"Could not verify subscription for user {user_id} in {channel_id}. Assuming not subscribed. Error: {e}")
                    missing_subscriptions.append(channel_id)
    
            if not missing_subscriptions:
                # --- USER IS SUBSCRIBED: Send the normal welcome message ---
                logger.info(f"User {user_id} is subscribed. Sending Web App link.")
    
                # Referral processing is now only done for subscribed users
                referral_code_found = None
                try:
                    command_parts = message.text.split(' ')
                    if len(command_parts) > 1 and command_parts[1].startswith('ref_'):
                        referral_code_found = command_parts[1]
                except Exception as e:
                    logger.error(f"Error parsing start parameter for user {user_id}: {e}")
    
                if referral_code_found:
                    logger.info(f"User {user_id} queueing background referral with code: {referral_code_found}")
                    api_payload = {
                        "user_id": user_id, "username": tg_user.username, "first_name": tg_user.first_name,
                        "last_name": tg_user.last_name, "referral_code": referral_code_found
                    }
                    thread = threading.Thread(target=register_referral_in_background, args=(api_payload,))
                    thread.start()
    
                markup = types.InlineKeyboardMarkup()
                web_app_info = types.WebAppInfo(url=WEBAPP_URL)
                app_button = types.InlineKeyboardButton(text="🎮 Open Case Hunter", web_app=web_app_info)
                markup.add(app_button)
    
                bot.send_photo(
                    message.chat.id,
                    photo="https://github.com/Vasiliy-katsyka/gifthunter/blob/main/IMG_20250825_191850_208.jpg?raw=true",
                    caption="Welcome to Case Hunter! 🎁\n\nTap the button below to start!",
                    reply_markup=markup
                )
            else:
                # --- USER IS NOT SUBSCRIBED: Send the subscription request message ---
                logger.info(f"User {user_id} is NOT subscribed to: {missing_subscriptions}. Sending subscription message.")
                
                markup = types.InlineKeyboardMarkup(row_width=1)
                # Create a button for each required channel
                for channel_handle in REQUIRED_CHANNELS:
                    channel_name = channel_handle.replace('@', '')
                    button = types.InlineKeyboardButton(f"➡️ Join {channel_name}", url=f"https://t.me/{channel_name}")
                    markup.add(button)
                
                # Add a "Check Subscription" button that re-sends the /start command
                check_button = types.InlineKeyboardButton("✅ Check Subscription", callback_data="check_subscription")
                markup.add(check_button)
                
                bot.send_message(
                    user_id,
                    "Для входа в приложение нужна подписка на наши каналы:",
                    reply_markup=markup
                )
            # --- END OF SUBSCRIPTION CHECK LOGIC ---
    
        except Exception as e:
            logger.warning(f"Could not process /start for {message.chat.id}. Error: {e}")
    
    
    @bot.callback_query_handler(func=lambda call: call.data == 'check_subscription')
    def check_subscription_callback(call: types.CallbackQuery):
        """
        Handles the 'Check Subscription' button press.
        It acknowledges the press and re-triggers the /start command logic.
        """
        try:
            bot.answer_callback_query(call.id, "Checking your subscription status...")
            # Re-run the start command logic for the user
            send_welcome(call.message) 
            # Optionally, delete the subscription message to clean up the chat
            bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)
        except Exception as e:
            logger.error(f"Error in check_subscription_callback for user {call.from_user.id}: {e}")
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith(('confirm_gift_deposit:', 'deny_gift_deposit:')))
    def handle_gift_deposit_confirmation(call: types.CallbackQuery):
        try:
            if call.from_user.id not in ADMIN_USER_IDS:
                bot.answer_callback_query(call.id, "Unauthorized action.")
                return
    
            action, _, data = call.data.partition(':')
            target_user_id_str, star_amount_str = data.split(':')
            target_user_id = int(target_user_id_str)
            star_amount = int(star_amount_str)
    
            if action == 'confirm_gift_deposit':
                ton_amount_to_add = Decimal(str(star_amount)) / Decimal(str(TON_TO_STARS_RATE_BACKEND))
                
                db = SessionLocal()
                try:
                    user_to_credit = db.query(User).filter(User.id == target_user_id).with_for_update().first()
                    if user_to_credit:
                        user_to_credit.ton_balance = float(Decimal(str(user_to_credit.ton_balance)) + ton_amount_to_add)
                        
                        # Log this to the new unified Deposit table
                        new_deposit_log = Deposit(
                            user_id=target_user_id,
                            ton_amount=float(ton_amount_to_add),
                            deposit_type='GIFT'
                        )
                        db.add(new_deposit_log)
                        db.commit()
                        
                        bot.answer_callback_query(call.id, f"Success! {star_amount} Stars added to user {target_user_id}.")
                        bot.edit_message_text(f"✅ Approved: {star_amount} Stars were added to user {target_user_id}.", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=None)
                        
                        # Notify the user
                        bot.send_message(target_user_id, f"✅ Ваш депозит подарком был подтвержден! На ваш баланс зачислено {star_amount} Stars.")
                    else:
                        bot.answer_callback_query(call.id, "Error: User not found in database.", show_alert=True)
                        bot.edit_message_text(f"⚠️ Failed: User {target_user_id} not found.", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=None)
                finally:
                    db.close()
    
            elif action == 'deny_gift_deposit':
                bot.answer_callback_query(call.id, "Deposit denied.")
                bot.edit_message_text(f"❌ Denied: Deposit for user {target_user_id} was rejected.", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=None)
                # Notify the user
                bot.send_message(target_user_id, f"❌ Ваш депозит подарком был отклонен. Пожалуйста, свяжитесь с поддержкой для уточнения деталей.")
    
        except Exception as e:
            logger.error(f"Error in handle_gift_deposit_confirmation: {e}", exc_info=True)
            bot.answer_callback_query(call.id, "An error occurred.", show_alert=True)

    # --- All other bot handlers now follow the same robust, top-level pattern ---

    def handle_statistics(message_to_edit):
        try:
            db = SessionLocal()
            today = dt.now(timezone.utc).date()
            daily_stats = db.query(DailyStats).filter(func.date(DailyStats.date) == today).first()
            all_time_stats = db.query(AllTimeStats).first()
    
            stats_text = "📊 *Statistics*\n\n*Today's Stats:*\n"
            if daily_stats:
                stats_text += f"- New Users: {daily_stats.new_users}\n- Cases Opened: {daily_stats.cases_opened}\n- TON Deposited: {daily_stats.ton_deposited:.2f}\n- TON Won: {daily_stats.ton_won:.2f}\n"
            else:
                stats_text += "- No stats for today yet.\n"
    
            stats_text += "\n*All-Time Stats:*\n"
            if all_time_stats:
                stats_text += f"- Total Users: {all_time_stats.total_users}\n- Total Cases Opened: {all_time_stats.total_cases_opened}\n- Total TON Deposited: {all_time_stats.total_ton_deposited:.2f}\n- Total TON Won: {all_time_stats.total_ton_won:.2f}\n"
            else:
                stats_text += "- No all-time stats yet.\n"
    
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="admin_back_to_menu"))
            bot.edit_message_text(stats_text, chat_id=message_to_edit.chat.id, message_id=message_to_edit.message_id, reply_markup=markup, parse_mode="Markdown")
    
        except Exception as e:
            logger.error(f"Error in handle_statistics: {e}")
            bot.send_message(message_to_edit.chat.id, "Error fetching statistics.")
        finally:
            if 'db' in locals(): db.close()

    def handle_mailing_list(message_to_edit):
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Create New Mailing", callback_data="admin_create_mailing"))
            markup.add(types.InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="admin_back_to_menu"))
            bot.edit_message_text("✉️ *Mailing List*\n\nCreate and send a message to your users.", chat_id=message_to_edit.chat.id, message_id=message_to_edit.message_id, reply_markup=markup, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error in handle_mailing_list: {e}")

    def process_mailing_message(message):
        try:
            message_text = message.text or message.caption
            file_id, file_type, button_text, button_url = None, None, None, None
            button_regex = re.compile(r'^\s*button:\s*(.+?)\s*\|\s*(https?://\S+)\s*$', re.MULTILINE | re.IGNORECASE)

            if message_text:
                match = button_regex.search(message_text)
                if match:
                    button_text, button_url = match.group(1).strip(), match.group(2).strip()
                    message_text = button_regex.sub('', message_text).strip()

            if message.photo: file_id, file_type = message.photo[-1].file_id, 'photo'
            elif message.video: file_id, file_type = message.video.file_id, 'video'
            elif message.animation: file_id, file_type = message.animation.file_id, 'animation'

            db = SessionLocal()
            new_mailing = MailingListMessage(
                message_text=message_text, file_id=file_id, file_type=file_type,
                button_text=button_text, button_url=button_url, sent_by=message.from_user.id
            )
            db.add(new_mailing)
            db.commit()
            db.refresh(new_mailing)

            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(f"🚀 Send Mailing #{new_mailing.id}", callback_data=f"admin_send_mailing_{new_mailing.id}"))
            bot.send_message(message.from_user.id, f"Mailing #{new_mailing.id} created. Ready to send.", reply_markup=markup)
        except Exception as e:
            logger.error(f"Error creating mailing: {e}")
            bot.send_message(message.from_user.id, "Error creating mailing.")
        finally:
            if 'db' in locals(): db.close()
            
    def send_mailing(message_id):
        # This function is already run in a thread, so errors here are less critical
        # but good practice to keep the try/except.
        db = SessionLocal()
        try:
            mailing_message = db.query(MailingListMessage).filter(MailingListMessage.id == message_id).first()
            if not mailing_message:
                logger.error(f"Mailing message with id {message_id} not found.")
                return

            users_to_send = db.query(User).all()
            for user in users_to_send:
                try: # Inner try/except to continue sending even if one user fails
                    reply_markup = None
                    if mailing_message.button_text and mailing_message.button_url:
                        markup = types.InlineKeyboardMarkup()
                        button = types.InlineKeyboardButton(text=mailing_message.button_text, url=mailing_message.button_url)
                        markup.add(button)
                        reply_markup = markup
                    
                    if mailing_message.file_type == 'photo': bot.send_photo(user.id, mailing_message.file_id, caption=mailing_message.message_text, reply_markup=reply_markup)
                    elif mailing_message.file_type == 'video': bot.send_video(user.id, mailing_message.file_id, caption=mailing_message.message_text, reply_markup=reply_markup)
                    elif mailing_message.file_type == 'animation': bot.send_animation(user.id, mailing_message.file_id, caption=mailing_message.message_text, reply_markup=reply_markup)
                    else: bot.send_message(user.id, mailing_message.message_text, reply_markup=reply_markup)
                    
                    db.add(MailingListUserStatus(message_id=message_id, user_id=user.id))
                    db.commit()
                except Exception as e:
                    logger.warning(f"Could not send mailing to user {user.id} (they may have blocked the bot). Error: {e}")
                    db.rollback()
        except Exception as e:
            logger.error(f"Critical error in send_mailing task for message_id {message_id}: {e}")
        finally:
            if 'db' in locals(): db.close()

    def process_new_promo_creation(message):
        try:
            if message.text == '/cancel':
                bot.clear_step_handler_by_chat_id(chat_id=message.chat.id)
                bot.reply_to(message, "Promocode creation cancelled.")
                admin_panel_command(message) # Go back to menu
                return
            
            parts = message.text.split()
            if not (3 <= len(parts) <= 4): raise ValueError("Incorrect format.")

            promo_name = parts[0]
            activations_str = parts[1]
            if activations_str.lower() in ['unlimited', '-1']: activations = -1
            else: activations = int(activations_str)
            if activations < -1: raise ValueError("Activations must be non-negative or -1 for unlimited.")

            prize_ton = float(parts[2])
            if prize_ton <= 0: raise ValueError("TON prize must be positive.")

            db = SessionLocal()
            if db.query(PromoCode).filter(PromoCode.code_text == promo_name).first():
                bot.reply_to(message, f"⚠️ Promocode '{promo_name}' already exists. Try a different name.")
                msg_reprompt = bot.send_message(message.chat.id, "Enter new promocode details or type /cancel.")
                bot.register_next_step_handler(msg_reprompt, process_new_promo_creation)
                return

            new_promo = PromoCode(code_text=promo_name, activations_left=activations, ton_amount=prize_ton)
            db.add(new_promo)
            db.commit()
            bot.reply_to(message, f"✅ Promocode '{promo_name}' created!")
        except Exception as e:
            logger.error(f"Error processing promocode: {e}")
            bot.reply_to(message, f"Invalid input. Please use format `name activations prize` or type /cancel.")
            msg_retry = bot.send_message(message.chat.id, "Enter details again or /cancel.")
            bot.register_next_step_handler(msg_retry, process_new_promo_creation)
        finally:
            if 'db' in locals(): db.close()
    
    def handle_view_all_promos(message_to_edit):
        try:
            db = SessionLocal()
            all_promos = db.query(PromoCode).order_by(PromoCode.created_at.desc()).all()
            markup = types.InlineKeyboardMarkup(row_width=2)
            if not all_promos: text_to_send = "No promocodes found."
            else:
                text_to_send = "Select a promocode to view details:"
                promo_buttons = [types.InlineKeyboardButton(p.code_text, callback_data=f"admin_promo_detail_{p.id}") for p in all_promos]
                for i in range(0, len(promo_buttons), 2): markup.row(*promo_buttons[i:i+2])
            
            markup.add(types.InlineKeyboardButton("⬅️ Back to Admin Menu", callback_data="admin_back_to_menu"))
            bot.edit_message_text(text_to_send, chat_id=message_to_edit.chat.id, message_id=message_to_edit.message_id, reply_markup=markup)
        except Exception as e:
            logger.error(f"Error in handle_view_all_promos: {e}")
        finally:
            if 'db' in locals(): db.close()

    def handle_view_promo_detail(message_to_edit, promo_id):
        try:
            db = SessionLocal()
            promo = db.query(PromoCode).filter(PromoCode.id == promo_id).first()
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("⬅️ Back to All Promocodes", callback_data="admin_view_promos"))
            markup.add(types.InlineKeyboardButton("🏠 Back to Admin Menu", callback_data="admin_back_to_menu"))

            if not promo: text_to_send = "Promocode not found."
            else:
                activations_text = "Unlimited" if promo.activations_left == -1 else str(promo.activations_left)
                created_date = promo.created_at.strftime('%Y-%m-%d %H:%M') if promo.created_at else 'N/A'
                text_to_send = (f"📜 *{promo.code_text}*\n\n🎁 Prize: {promo.ton_amount:.4f} TON\n"
                                f"🔄 Left: {activations_text}\n🗓️ Created: {created_date}")
            
            bot.edit_message_text(text_to_send, chat_id=message_to_edit.chat.id, message_id=message_to_edit.message_id, reply_markup=markup, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error in handle_view_promo_detail: {e}")
        finally:
            if 'db' in locals(): db.close()

    @bot.message_handler(commands=['admin'])
    def admin_panel_command(message):
        try:
            if message.chat.id not in ADMIN_USER_IDS:
                bot.reply_to(message, "You are not authorized.")
                return
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("🆕 New Promocode", callback_data="admin_new_promo"),
                       types.InlineKeyboardButton("📋 All Promocodes", callback_data="admin_view_promos"),
                       types.InlineKeyboardButton("📊 Statistics", callback_data="admin_stats"),
                       types.InlineKeyboardButton("✉️ Mailing List", callback_data="admin_mailing_list"))
            bot.send_message(message.chat.id, "👑 Admin Panel 👑", reply_markup=markup)
        except Exception as e:
            logger.error(f"Error in admin_panel_command: {e}")
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith('admin_'))
    def admin_callback_handler(call):
        try:
            if call.from_user.id not in ADMIN_USER_IDS:
                bot.answer_callback_query(call.id, "Unauthorized.")
                return
        
            bot.answer_callback_query(call.id) # Answer immediately
            action = call.data
            
            if action == "admin_mailing_list": handle_mailing_list(call.message)
            elif action == "admin_stats": handle_statistics(call.message)
            elif action == "admin_view_promos": handle_view_all_promos(call.message)
            elif action == "admin_new_promo":
                msg = bot.send_message(call.from_user.id, "Enter promocode: `name activations prize_ton` or /cancel.", parse_mode="Markdown")
                bot.register_next_step_handler(msg, process_new_promo_creation)
            elif action == "admin_create_mailing":
                msg = bot.send_message(call.from_user.id, "Send the message for mailing (text/media + optional button `button: Text | url`) or /cancel.", parse_mode="Markdown")
                bot.register_next_step_handler(msg, process_mailing_message)
            elif action.startswith("admin_promo_detail_"):
                handle_view_promo_detail(call.message, int(action.split('_')[-1]))
            elif action.startswith("admin_send_mailing_"):
                bot.edit_message_text(f"Mailing #{int(action.split('_')[-1])} is now being sent in the background...", chat_id=call.message.chat.id, message_id=call.message.message_id)
                threading.Thread(target=send_mailing, args=(int(action.split('_')[-1]),)).start()
            elif action == "admin_back_to_menu":
                admin_panel_command(call.message)
        except Exception as e:
            logger.error(f"Error in admin_callback_handler: {e}")

    @bot.pre_checkout_query_handler(func=lambda query: True)
    def pre_checkout_process(pre_checkout_query: types.PreCheckoutQuery):
        try:
            bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
        except Exception as e:
            logger.error(f"Error in pre_checkout_process: {e}")
    
    # --- Find and REPLACE the entire successful_payment_process function ---
    
    # --- Find and REPLACE the entire successful_payment_process function ---
    @bot.message_handler(content_types=['successful_payment'])
    def successful_payment_process(message: types.Message):
        try:
            payment_info = message.successful_payment
            currency = payment_info.currency
            total_amount = payment_info.total_amount
            user_id = message.from_user.id
    
            logger.info(f"Received successful payment from user {user_id}: {total_amount} {currency}.")
    
            if currency == "XTR":
                stars_purchased = total_amount
                ton_equivalent_deposit = Decimal(str(stars_purchased)) / Decimal(str(TON_TO_STARS_RATE_BACKEND))
    
                db = SessionLocal()
                try:
                    user = db.query(User).filter(User.id == user_id).with_for_update().first()
                    if not user:
                        logger.error(f"User {user_id} not found after successful payment!")
                        return
    
                    user.ton_balance = float(Decimal(str(user.ton_balance)) + ton_equivalent_deposit)
                    
                    # --- START OF CHANGE ---
                    # Log this successful Star deposit to our new unified table
                    new_deposit_log = Deposit(
                        user_id=user_id,
                        ton_amount=float(ton_equivalent_deposit),
                        deposit_type='STARS'
                    )
                    db.add(new_deposit_log)
                    # --- END OF CHANGE ---
    
                    if user.referred_by_id:
                        referrer = db.query(User).filter(User.id == user.referred_by_id).with_for_update().first()
                        if referrer:
                            referral_rate = DEFAULT_REFERRAL_RATE
                            if referrer.username and referrer.username.lower() in (name.lower() for name in SPECIAL_REFERRAL_RATES.keys()):
                                referral_rate = SPECIAL_REFERRAL_RATES[referrer.username.lower()]
                            
                            referral_bonus_ton = ton_equivalent_deposit * referral_rate
                            current_pending = Decimal(str(referrer.referral_earnings_pending))
                            referrer.referral_earnings_pending = float(current_pending + referral_bonus_ton)
                            logger.info(f"Referral bonus of {referral_bonus_ton:.4f} TON credited to referrer {referrer.id} for deposit by user {user.id}.")
    
                    db.commit()
                    logger.info(f"Credited user {user_id} with {ton_equivalent_deposit:.4f} TON for {stars_purchased} Stars.")
                    bot.send_message(user_id, f"✅ Thank you! Your payment for {stars_purchased} Stars was successful. Your balance has been updated.")
    
                except Exception as e:
                    db.rollback()
                    logger.error(f"DATABASE ERROR processing Stars payment for {user_id}: {e}", exc_info=True)
                    bot.send_message(user_id, "⚠️ There was an issue processing your payment. Please contact support.")
                finally:
                    db.close()
        except Exception as e:
            logger.error(f"Error in successful_payment_process: {e}")

    @bot.message_handler(commands=['cancel'])
    def cancel_operation(message):
        try:
            if message.chat.id in ADMIN_USER_IDS:
                bot.clear_step_handler_by_chat_id(chat_id=message.chat.id)
                bot.reply_to(message, "Operation cancelled.")
                admin_panel_command(message)
        except Exception as e:
            logger.error(f"Error in cancel_operation: {e}")

    @bot.message_handler(func=lambda message: True)
    def echo_all(message):
        try:
            logger.info(f"Received non-command from {message.chat.id}: {message.text[:50]}")
            bot.reply_to(message, "Send /start to open Case Hunter")
        except Exception as e:
            logger.warning(f"Could not reply to echo_all for user {message.chat.id}. Error: {e}")


# --- Webhook Setup Function (to be called from your main app setup) ---
# ... (the rest of your file, including the webhook setup and Flask API routes, remains the same) ...

# --- Webhook Setup Function (to be called from your main app setup) ---
# You need to pass your Flask 'app' instance to this function to register the route.
def setup_telegram_webhook(flask_app_instance):
    if not bot:
        logger.error("Telegram bot instance is not initialized (BOT_TOKEN missing?). Webhook cannot be set.")
        return

    # Path for the webhook - using the bot token makes it secret
    WEBHOOK_PATH = f'/{BOT_TOKEN}'
    # The full URL for the webhook
    # Render provides RENDER_EXTERNAL_HOSTNAME. If not, you'd use your specific server URL.
    render_hostname = os.getenv('RENDER_EXTERNAL_HOSTNAME')
    if render_hostname:
        WEBHOOK_URL_BASE = f"https://{render_hostname}"
    else:
        # Fallback to your explicitly provided server URL if RENDER_EXTERNAL_HOSTNAME is not available
        WEBHOOK_URL_BASE = "https://case-hznb.onrender.com"
        logger.warning(f"RENDER_EXTERNAL_HOSTNAME not found, using manually configured URL: {WEBHOOK_URL_BASE}")

    FULL_WEBHOOK_URL = f"{WEBHOOK_URL_BASE}{WEBHOOK_PATH}"

    # Define the webhook handler route within the Flask app context
    @flask_app_instance.route(WEBHOOK_PATH, methods=['POST'])
    def webhook_handler():
        if flask_request.headers.get('content-type') == 'application/json':
            json_string = flask_request.get_data().decode('utf-8')
            update = telebot.types.Update.de_json(json_string)
            logger.debug(f"Webhook received update: {update.update_id}")
            bot.process_new_updates([update])
            return '', 200
        else:
            logger.warning("Webhook received non-JSON request.")
            flask_abort(403)
        return "Webhook handler setup.", 200 # Should not be reached if POST with JSON

    # Set the webhook with Telegram API
    # This should run once when your application starts up.
    # It's good practice to check if it's already set correctly.
    try:
        current_webhook_info = bot.get_webhook_info()
        if current_webhook_info.url != FULL_WEBHOOK_URL:
            logger.info(f"Current webhook is '{current_webhook_info.url}', attempting to set to: {FULL_WEBHOOK_URL}")
            bot.remove_webhook()
            time.sleep(0.5) # Give Telegram a moment
            success = bot.set_webhook(url=FULL_WEBHOOK_URL)
            if success:
                logger.info(f"Telegram webhook set successfully to {FULL_WEBHOOK_URL}")
            else:
                logger.error(f"Failed to set Telegram webhook to {FULL_WEBHOOK_URL}. Current info: {bot.get_webhook_info()}")
        else:
            logger.info(f"Telegram webhook already set correctly to: {FULL_WEBHOOK_URL}")
    except Exception as e:
        logger.error(f"Error during Telegram webhook setup: {e}", exc_info=True)

# --- Tonnel Gift Sender (AES-256-CBC compatible with CryptoJS) ---
SALT_SIZE = 8
KEY_SIZE = 32
IV_SIZE = 16

def derive_key_and_iv(passphrase: str, salt: bytes, key_length: int, iv_length: int) -> tuple[bytes, bytes]:
    derived = b''
    hasher = hashlib.md5()
    hasher.update(passphrase.encode('utf-8'))
    hasher.update(salt)
    derived_block = hasher.digest()
    derived += derived_block
    while len(derived) < key_length + iv_length:
        hasher = hashlib.md5()
        hasher.update(derived_block)
        hasher.update(passphrase.encode('utf-8'))
        hasher.update(salt)
        derived_block = hasher.digest()
        derived += derived_block
    key = derived[:key_length]
    iv = derived[key_length : key_length + iv_length]
    return key, iv

def encrypt_aes_cryptojs_compat(plain_text: str, secret_passphrase: str) -> str:
    salt = get_random_bytes(SALT_SIZE)
    key, iv = derive_key_and_iv(secret_passphrase, salt, KEY_SIZE, IV_SIZE)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plain_text_bytes = plain_text.encode('utf-8')
    padded_plain_text = pad(plain_text_bytes, AES.block_size, style='pkcs7')
    ciphertext = cipher.encrypt(padded_plain_text)
    salted_ciphertext = b"Salted__" + salt + ciphertext
    encrypted_base64 = base64.b64encode(salted_ciphertext).decode('utf-8')
    return encrypted_base64

class TonnelGiftSender:
    def __init__(self, sender_auth_data: str, gift_secret_passphrase: str):
        self.passphrase_secret = gift_secret_passphrase
        self.authdata = sender_auth_data
        self._session_instance: AsyncSession | None = None

    async def _get_session(self) -> AsyncSession:
        if self._session_instance is None:
            # Try a newer impersonation target
            self._session_instance = AsyncSession(impersonate="chrome120") # Changed from chrome110
        return self._session_instance

    async def _close_session_if_open(self):
        if self._session_instance:
            try:
                await self._session_instance.close()
            except Exception as e_close:
                logger.error(f"Error while closing AsyncSession: {e_close}")
            finally:
                self._session_instance = None

    async def _make_request(self, method: str, url: str, headers: dict | None = None, json_payload: dict | None = None, timeout: int = 30, is_initial_get: bool = False):
        session = await self._get_session()
        response_obj = None
        try:
            request_kwargs = {"headers": headers, "timeout": timeout}
            if json_payload is not None and method.upper() == "POST":
                request_kwargs["json"] = json_payload

            if method.upper() == "GET":
                response_obj = await session.get(url, **request_kwargs)
            elif method.upper() == "POST":
                response_obj = await session.post(url, **request_kwargs)
            elif method.upper() == "OPTIONS":
                response_obj = await session.options(url, **request_kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if method.upper() == "OPTIONS":
                if 200 <= response_obj.status_code < 300:
                    return {"status": "options_ok"}
                else:
                    err_text_options = await response_obj.text()
                    logger.error(f"Tonnel API OPTIONS {url} failed: {response_obj.status_code}. Resp: {err_text_options[:500]}")
                    response_obj.raise_for_status()
                    return {"status": "error", "message": f"OPTIONS failed: {response_obj.status_code}"}

            response_obj.raise_for_status()

            if response_obj.status_code == 204:
                return None

            content_type = response_obj.headers.get("Content-Type", "").lower()
            if "application/json" in content_type:
                try:
                    return response_obj.json()
                except json.JSONDecodeError as je_err_inner:
                    err_text_json_decode = await response_obj.text()
                    logger.error(f"Tonnel API JSONDecodeError for {method} {url}: {je_err_inner}. Body: {err_text_json_decode[:500]}")
                    return {"status": "error", "message": "Invalid JSON in response", "raw_text": err_text_json_decode[:500]}
            else:
                if is_initial_get:
                    return {"status": "get_ok_non_json"}
                else:
                    responseText = await response_obj.text()
                    logger.warning(f"Tonnel API {method} {url} - Non-JSON (Type: {content_type}). Text: {responseText[:200]}")
                    return {"status": "error", "message": "Response not JSON", "content_type": content_type, "text_preview": responseText[:200]}

        except RequestsError as re_err:
            logger.error(f"Tonnel API RequestsError ({method} {url}): {re_err}")
            raise
        except json.JSONDecodeError as je_err:
            logger.error(f"Tonnel API JSONDecodeError (outer) for {method} {url}: {je_err}")
            raise ValueError(f"Failed to decode JSON from {url}") from je_err
        except Exception as e_gen:
            logger.error(f"Tonnel API general request error ({method} {url}): {type(e_gen).__name__} - {e_gen}")
            raise

    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int):
        if not self.authdata:
            return {"status": "error", "message": "Tonnel sender not configured."}

        try:
            # Step 1: Initial GET request to marketplace.tonnel.network to establish session/cookies
            await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)

            # Step 2: Find the cheapest available gift item on Tonnel Market
            
            # Initialize common filter parts
            filter_dict = {
                "price": {"$exists": True},
                "refunded": {"$ne": True},
                "buyer": {"$exists": False},
                "export_at": {"$exists": True},
                "asset": "TON",
            }

            # Check if the requested item is a Kissed Frog variant and needs the 'model' filter
            if gift_item_name in KISS_FROG_MODEL_STATIC_PERCENTAGES:
                # It's a Kissed Frog variant, so add the specific gift_name and model fields
                static_percentage_val = KISS_FROG_MODEL_STATIC_PERCENTAGES[gift_item_name]
                
                # Format the percentage to one decimal place if it's .0, otherwise as is.
                # This ensures "1.0" becomes "1%", not "1.0%".
                # rstrip('0').rstrip('.') will handle 1.0 -> 1 and 0.5 -> 0.5
                formatted_percentage = f"{static_percentage_val:.1f}".rstrip('0').rstrip('.')

                filter_dict["gift_name"] = "Kissed Frog"  # Always "Kissed Frog" for variants
                filter_dict["model"] = f"{gift_item_name} ({formatted_percentage}%)"
            else:
                # It's a regular NFT, use its name directly in the 'gift_name' filter
                filter_dict["gift_name"] = gift_item_name

            filter_str = json.dumps(filter_dict)

            page_gifts_payload = {"filter":filter_str,"limit":10,"page":1,"sort":'{"price":1,"gift_id":-1}'}
            pg_headers_options = {"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type","Origin":"https://tonnel-gift.vercel.app","Referer":"https://tonnel-gift.vercel.app/"}
            pg_headers_post = {"Content-Type":"application/json","Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}

            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_options)
            gifts_found_response = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_post, json_payload=page_gifts_payload)

            if not isinstance(gifts_found_response, list):
                return {"status":"error","message":f"Could not fetch gift list: {gifts_found_response.get('message','API error') if isinstance(gifts_found_response,dict) else 'Format error'}"}
            if not gifts_found_response:
                return {"status":"error","message":f"No '{gift_item_name}' gifts available on Tonnel marketplace."}
            
            low_gift = gifts_found_response[0]

            logger.info(f"Tonnel gift found for '{gift_item_name}': {json.dumps(low_gift, indent=2)}")
            
            # Step 3: Verify the receiver's Telegram ID with Tonnel (optional but good practice for robustness)
            user_info_payload = {"authData":self.authdata,"user":receiver_telegram_id}
            ui_common_headers = {"Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}
            ui_options_headers = {**ui_common_headers,"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type"}
            ui_post_headers = {**ui_common_headers,"Content-Type":"application/json"}
            
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_options_headers)
            user_check_resp = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_post_headers, json_payload=user_info_payload)

            if not isinstance(user_check_resp, dict) or user_check_resp.get("status") != "success":
                return {"status":"error","message":f"Tonnel user check failed: {user_check_resp.get('message','User error') if isinstance(user_check_resp,dict) else 'Unknown error'}"}

            # Step 4: Purchase and send the gift
            encrypted_ts = encrypt_aes_cryptojs_compat(f"{int(time.time())}", self.passphrase_secret)
            buy_gift_url = f"https://gifts.coffin.meme/api/buyGift/{low_gift['gift_id']}"
            buy_payload = {"anonymously":True,"asset":"TON","authData":self.authdata,"price":low_gift['price'],"receiver":receiver_telegram_id,"showPrice":False,"timestamp":encrypted_ts}
            buy_common_headers = {"Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/","Host":"gifts.coffin.meme"}
            buy_options_headers = {**buy_common_headers,"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type"}
            buy_post_headers = {**buy_common_headers,"Content-Type":"application/json"}

            await self._make_request(method="OPTIONS", url=buy_gift_url, headers=buy_options_headers)
            purchase_resp = await self._make_request(method="POST", url=buy_gift_url, headers=buy_post_headers, json_payload=buy_payload, timeout=90)

            if isinstance(purchase_resp, dict) and purchase_resp.get("status") == "success":
                return {"status":"success","message":f"Gift '{gift_item_name}' sent!","details":purchase_resp}
            else:
                return {"status":"error","message":f"Tonnel transfer failed: {purchase_resp.get('message','Purchase error') if isinstance(purchase_resp,dict) else 'Unknown error'}"}

        except Exception as e:
            logger.error(f"Tonnel error sending gift '{gift_item_name}' to {receiver_telegram_id}: {type(e).__name__} - {e}", exc_info=True)
            return {"status":"error","message":f"Unexpected error during Tonnel withdrawal: {str(e)}"}
        finally:
            await self._close_session_if_open()

    async def fetch_gift_listings(self, gift_item_name: str, limit: int = 5) -> list:
        """Fetches up to 'limit' available listings for a specific gift_item_name from Tonnel Market."""
        if not self.authdata: # authdata might not be strictly needed for just fetching listings, but good for consistency
            logger.warning("Tonnel fetch_gift_listings: sender not configured (authdata missing).")
            # Decide if you want to proceed or return error. For now, proceed.
            # return {"status": "error", "message": "Tonnel sender not configured."}


        # Step 1: Initial GET request if needed (usually done once per session lifecycle)
        await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)

        filter_dict = {
            "price": {"$exists": True},
            "refunded": {"$ne": True},
            "buyer": {"$exists": False},
            "export_at": {"$exists": True},
            "asset": "TON",
        }
        if gift_item_name in KISS_FROG_MODEL_STATIC_PERCENTAGES:
            static_percentage_val = KISS_FROG_MODEL_STATIC_PERCENTAGES[gift_item_name]
            formatted_percentage = f"{static_percentage_val:.1f}".rstrip('0').rstrip('.')
            filter_dict["gift_name"] = "Kissed Frog"
            filter_dict["model"] = f"{gift_item_name} ({formatted_percentage}%)"
        else:
            filter_dict["gift_name"] = gift_item_name
        
        filter_str = json.dumps(filter_dict)
        page_gifts_payload = {"filter": filter_str, "limit": limit, "page": 1, "sort": '{"price":1,"gift_id":-1}'} # Sort by price ascending
        
        pg_headers_options = {"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type","Origin":"https://tonnel-gift.vercel.app","Referer":"https://tonnel-gift.vercel.app/"}
        pg_headers_post = {"Content-Type":"application/json","Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}


        await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_options)
        gifts_found_response = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_post, json_payload=page_gifts_payload)

        if not isinstance(gifts_found_response, list):
            logger.error(f"Tonnel fetch_gift_listings: Could not fetch gift list for '{gift_item_name}'. Response: {gifts_found_response}")
            return [] # Return empty list on error or non-list response
        
        # Return the raw list of gifts from Tonnel
        # Frontend will handle formatting for display (gift_num for image, etc.)
        return gifts_found_response[:limit]


    async def purchase_specific_gift(self, chosen_gift_details: dict, receiver_telegram_id: int):
        """Purchases a specific gift using its details (gift_id, price) from Tonnel."""
        if not self.authdata:
            return {"status": "error", "message": "Tonnel sender not configured."}
        if not chosen_gift_details or 'gift_id' not in chosen_gift_details or 'price' not in chosen_gift_details:
            return {"status": "error", "message": "Invalid chosen gift details provided."}

        try:
            # Initial GET may not be needed if session is kept alive from fetch_gift_listings
            # await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)

            # User check (optional, but can be good)
            user_info_payload = {"authData":self.authdata,"user":receiver_telegram_id}
            ui_common_headers = {"Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}
            ui_options_headers = {**ui_common_headers,"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type"}
            ui_post_headers = {**ui_common_headers,"Content-Type":"application/json"}
            
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_options_headers)
            user_check_resp = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_post_headers, json_payload=user_info_payload)

            if not isinstance(user_check_resp, dict) or user_check_resp.get("status") != "success":
                return {"status":"error","message":f"Tonnel user check failed for receiver {receiver_telegram_id}: {user_check_resp.get('message','User error') if isinstance(user_check_resp,dict) else 'Unknown error'}"}

            # Purchase the specific gift
            encrypted_ts = encrypt_aes_cryptojs_compat(f"{int(time.time())}", self.passphrase_secret)
            buy_gift_url = f"https://gifts.coffin.meme/api/buyGift/{chosen_gift_details['gift_id']}"
            
            buy_payload = {
                "anonymously": True,
                "asset": "TON",
                "authData": self.authdata,
                "price": chosen_gift_details['price'], # Use price from chosen gift
                "receiver": receiver_telegram_id,
                "showPrice": False,
                "timestamp": encrypted_ts
            }
            buy_common_headers = {"Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/","Host":"gifts.coffin.meme"}
            buy_options_headers = {**buy_common_headers,"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type"}
            buy_post_headers = {**buy_common_headers,"Content-Type":"application/json"}

            await self._make_request(method="OPTIONS", url=buy_gift_url, headers=buy_options_headers)
            purchase_resp = await self._make_request(method="POST", url=buy_gift_url, headers=buy_post_headers, json_payload=buy_payload, timeout=90)
            
            if isinstance(purchase_resp, dict) and purchase_resp.get("status") == "success":
                return {"status":"success","message":f"Gift purchased and sent!","details":purchase_resp}
            else:
                # Log the raw payload and response for debugging failed purchases
                logger.error(f"Tonnel purchase_specific_gift failed. Payload: {buy_payload}, Response: {purchase_resp}")
                return {"status":"error","message":f"Tonnel transfer failed: {purchase_resp.get('message','Purchase error') if isinstance(purchase_resp,dict) else 'Unknown error'}"}

        except Exception as e:
            logger.error(f"Tonnel error purchasing specific gift: {type(e).__name__} - {e}", exc_info=True)
            return {"status":"error","message":f"Unexpected error during Tonnel purchase: {str(e)}"}
        # Removed finally block with _close_session_if_open to allow session reuse if desired by calling logic.
        # The calling API endpoint wrapper should handle session closing.


# --- Gift Data and Image Mapping ---
TON_PRIZE_IMAGE_DEFAULT = "https://case-bot.com/images/actions/ton.svg"

GIFT_NAME_TO_ID_MAP_PY = {
  "Santa Hat": "5983471780763796287","Signet Ring": "5936085638515261992","Precious Peach": "5933671725160989227","Plush Pepe": "5936013938331222567",
  "Spiced Wine": "5913442287462908725","Jelly Bunny": "5915502858152706668","Durov's Cap": "5915521180483191380","Perfume Bottle": "5913517067138499193",
  "Eternal Rose": "5882125812596999035","Berry Box": "5882252952218894938","Vintage Cigar": "5857140566201991735","Magic Potion": "5846226946928673709",
  "Kissed Frog": "5845776576658015084","Hex Pot": "5825801628657124140","Evil Eye": "5825480571261813595","Sharp Tongue": "5841689550203650524",
  "Trapped Heart": "5841391256135008713","Skull Flower": "5839038009193792264","Scared Cat": "5837059369300132790","Spy Agaric": "5821261908354794038",
  "Homemade Cake": "5783075783622787539","Genie Lamp": "5933531623327795414","Lunar Snake": "6028426950047957932","Party Sparkler": "6003643167683903930",
  "Jester Hat": "5933590374185435592","Witch Hat": "5821384757304362229","Hanging Star": "5915733223018594841","Love Candle": "5915550639663874519",
  "Cookie Heart": "6001538689543439169","Desk Calendar": "5782988952268964995","Jingle Bells": "6001473264306619020","Snow Mittens": "5980789805615678057",
  "Voodoo Doll": "5836780359634649414","Mad Pumpkin": "5841632504448025405","Hypno Lollipop": "5825895989088617224","B-Day Candle": "5782984811920491178",
  "Bunny Muffin": "5935936766358847989","Astral Shard": "5933629604416717361","Flying Broom": "5837063436634161765","Crystal Ball": "5841336413697606412",
  "Eternal Candle": "5821205665758053411","Swiss Watch": "5936043693864651359","Ginger Cookie": "5983484377902875708","Mini Oscar": "5879737836550226478",
  "Lol Pop": "5170594532177215681","Ion Gem": "5843762284240831056","Star Notepad": "5936017773737018241","Loot Bag": "5868659926187901653",
  "Love Potion": "5868348541058942091","Toy Bear": "5868220813026526561","Diamond Ring": "5868503709637411929","Sakura Flower": "5167939598143193218",
  "Sleigh Bell": "5981026247860290310","Top Hat": "5897593557492957738","Record Player": "5856973938650776169","Winter Wreath": "5983259145522906006",
  "Snow Globe": "5981132629905245483","Electric Skull": "5846192273657692751","Tama Gadget": "6023752243218481939","Candy Cane": "6003373314888696650",
  "Neko Helmet": "5933793770951673155","Jack-in-the-Box": "6005659564635063386","Easter Egg": "5773668482394620318",
  "Bonded Ring": "5870661333703197240", "Pet Snake": "6023917088358269866", "Snake Box": "6023679164349940429",
  "Xmas Stocking": "6003767644426076664", "Big Year": "6028283532500009446",
    "Holiday Drink": "6003735372041814769",
    "Gem Signet": "5859442703032386168",
    "Light Sword": "5897581235231785485",
    "Restless Jar": "5870784783948186838",
    "Nail Bracelet": "5870720080265871962",
    "Heroic Helmet": "5895328365971244193",
    "Bow Tie": "5895544372761461960",
    "Heart Locket": "5868455043362980631",
    "Lush Bouquet": "5871002671934079382",
    "Whip Cupcake": "5933543975653737112",
    "Joyful Bundle": "5870862540036113469",
    "Cupid Charm": "5868561433997870501",
    "Valentine Box": "5868595669182186720",
    "Snoop Dogg": "6014591077976114307",
    "Swag Bag": "6012607142387778152",
    "Snoop Cigar": "6012435906336654262",
    "Low Rider": "6014675319464657779",
    "Westside Sign": "6014697240977737490",
}


def generate_image_filename_from_name(name_str: str) -> str:
    """
    Generates a filename or direct CDN URL for a gift image based on its name.
    Prioritizes Tonnel CDN, then local repo paths for special cases, then generic conversion.
    """
    if not name_str: return 'placeholder.png'

    if name_str == "placeholder_nothing.png": return 'https://images.emojiterra.com/mozilla/512px/274c.png'

    if "TON" in name_str.upper() and ("PRIZE" in name_str.upper() or name_str.replace('.', '', 1).replace(' TON', '').strip().replace(',', '').isdigit()):
        return TON_PRIZE_IMAGE_DEFAULT

    gift_id = GIFT_NAME_TO_ID_MAP_PY.get(name_str)
    if gift_id:
        return f"https://cdn.changes.tg/gifts/originals/{gift_id}/Original.png"

    if name_str in KISSED_FROG_VARIANT_FLOORS:
        return f"https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/{name_str.replace(' ', '%20')}.png"

    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Vintage Cigar": return "Vintage-Cigar.png"
    name_str_rep = name_str.replace('-', '_')
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png'

    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""))
    filename = re.sub(r'-+', '-', cleaned)
    if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
        filename += '.png'
    return filename

# --- Floor Prices for all known NFTs (and Kissed Frog variants) ---
UPDATED_FLOOR_PRICES = {
    'Plush Pepe': 3889.0,    
    'Neko Helmet': 22.7,
    'Sharp Tongue': 29.8,
    "Durov's Cap": 609.0,    
    'Voodoo Doll': 13.9,
    'Vintage Cigar': 20.0,    
    'Astral Shard': 80.0,      
    'Scared Cat': 36.0,
    'Swiss Watch': 29.0,      
    'Perfume Bottle': 71.0,     
    'Precious Peach': 246.0,    
    'Toy Bear': 16.3,
    'Genie Lamp': 46.0,        
    'Loot Bag': 80.0,           
    'Kissed Frog': 24.0,        
    'Electric Skull': 24.9,
    'Diamond Ring': 14.0,       
    'Mini Oscar': 74.5,
    'Party Sparkler': 1.7,
    'Homemade Cake': 1.5,
    'Cookie Heart': 1.6,
    'Jack-in-the-box': 1.7,
    'Skull Flower': 5.7,
    'Lol Pop': 1.2,             
    'Hypno Lollipop': 1.78,      
    'Desk Calendar': 1.1,       
    'B-Day Candle': 1.1,
    'Record Player': 9.1,
    'Jelly Bunny': 2.6,
    'Tama Gadget': 1.6,
    'Snow Globe': 2.1,          
    'Eternal Rose': 12.0,
    'Love Potion': 8,
    'Top Hat': 7.7,
    'Berry Box': 3.4,
    'Bunny Muffin': 3,
    'Candy Cane': 1.4,
    'Crystal Ball': 6.0,
    'Easter Egg': 2.6,
    'Eternal Candle': 2.3,
    'Evil Eye': 3.1,
    'Flying Broom': 8.4,
    'Ginger Cookie': 1.7,
    'Hanging Star': 4.1,
    'Hex Pot': 2.2,
    'Ion Gem': 62.9,
    'Jester Hat': 1.6,
    'Jingle Bells': 1.7,
    'Love Candle': 6.0,
    'Lunar Snake': 1.3,
    'Mad Pumpkin': 12,
    'Magic Potion': 52.0,       
    'Pet Snake': 1.4,
    'Sakura Flower': 4.1,
    'Santa Hat': 2.0,
    'Signet Ring': 22.8,
    'Sleigh Bell': 5.0,
    'Snow Mittens': 2.9,
    'Spiced Wine': 2.1,
    'Spy Agaric': 2.9,
    'Star Notepad': 1.9,
    'Trapped Heart': 6.4,
    'Winter Wreath': 1.6,
    "Big Year": 1.5,
    "Snake Box": 1.3,
    "Bonded Ring": 43.5,
    "Xmas Stocking": 1.3,
    "Holiday Drink": 1.8,
    "Gem Signet": 55.9,
    "Light Sword": 2.8,
    "Restless Jar": 2.3,
    "Nail Bracelet": 107.8,
    "Heroic Helmet": 188.0,
    "Bow Tie": 2.9,
    "Heart Locket": 1170.0,
    "Lush Bouquet": 2.4,
    "Whip Cupcake": 1.4,
    "Joyful Bundle": 2.6,
    "Cupid Charm": 9.0,
    "Valentine Box": 3.7,
    "Snoop Dogg": 1.6,
    "Swag Bag": 1.8,
    "Snoop Cigar": 4.4,
    "Low Rider": 21.7,
    "Westside Sign": 44.5,
    'Heart': 0.06,
    'Bear': 0.06,
    'Rose': 0.1,
    'Rocket': 0.2,
    'Bottle': 0.2,
    # Add Light Sword and Gem Signet if they have defined prices
    # "Light Sword": XX.X,
    # "Gem Signet": XX.X
}

KISSED_FROG_VARIANT_FLOORS = {
    "Happy Pepe":500.0,"Tree Frog":150.0,"Brewtoad":150.0,"Puddles":150.0,"Honeyhop":150.0,"Melty Butter":150.0,
    "Lucifrog":150.0,"Zodiak Croak":150.0,"Count Croakula":150.0,"Lilie Pond":150.0,"Sweet Dream":150.0,
    "Frogmaid":150.0,"Rocky Hopper":150.0,"Icefrog":45.0,"Lava Leap":45.0,"Toadstool":45.0,"Desert Frog":45.0,
    "Cupid":45.0,"Hopberry":45.0,"Ms. Toad":45.0,"Trixie":45.0,"Prince Ribbit":45.0,"Pond Fairy":45.0,
    "Boingo":45.0,"Tesla Frog":45.0,"Starry Night":30.0,"Silver":30.0,"Ectofrog":30.0,"Poison":30.0,
    "Minty Bloom":30.0,"Sarutoad":30.0,"Void Hopper":30.0,"Ramune":30.0,"Lemon Drop":30.0,"Ectobloom":30.0,
    "Duskhopper":30.0,"Bronze":30.0,"Lily Pond":19.0,"Toadberry":19.0,"Frogwave":19.0,"Melon":19.0,
    "Sky Leaper":19.0,"Frogtart":19.0,"Peach":19.0,"Sea Breeze":19.0,"Lemon Juice":19.0,"Cranberry":19.0,
    "Tide Pod":19.0,"Brownie":19.0,"Banana Pox":19.0
}
UPDATED_FLOOR_PRICES.update(KISSED_FROG_VARIANT_FLOORS)


# --- RTP Calculation Functions ---
def calculate_rtp_probabilities(case_data, all_floor_prices):
    """
    Calculates and adjusts prize probabilities for a given case data
    to achieve a target RTP, maintaining relative prize probability ratios.
    """
    case_price = Decimal(str(case_data['priceTON']))
    target_ev = case_price * RTP_TARGET

    prizes = []
    for p_info in case_data['prizes']:
        prize_name = p_info['name']
        floor_price = Decimal(str(all_floor_prices.get(prize_name, 0)))
        image_filename = p_info.get('imageFilename', generate_image_filename_from_name(prize_name)) # Preserve image filename
        is_ton_prize = p_info.get('is_ton_prize', False) # Preserve is_ton_prize
        prizes.append({'name': prize_name, 'probability': Decimal(str(p_info['probability'])), 'floor_price': floor_price, 'imageFilename': image_filename, 'is_ton_prize': is_ton_prize})

    if not prizes or all(p['floor_price'] == Decimal('0') for p in prizes):
        logger.warning(f"Case {case_data['id']} has no valuable prizes or no prizes. Normalizing probabilities without EV adjustment.")
        total_original_prob = sum(p['probability'] for p in prizes)
        normalized_prizes = []
        if total_original_prob > 0:
            for p in prizes:
                normalized_prizes.append({
                    'name': p['name'],
                    'probability': float((p['probability'] / total_original_prob).quantize(Decimal('1E-7'))),
                    'floor_price': float(p['floor_price']),
                    'imageFilename': p['imageFilename'],
                    'is_ton_prize': p['is_ton_prize']
                })
        else:
            if prizes:
                equal_prob = Decimal('1.0') / len(prizes)
                for p in prizes:
                    normalized_prizes.append({
                        'name': p['name'],
                        'probability': float(equal_prob.quantize(Decimal('1E-7'))),
                        'floor_price': float(p['floor_price']),
                        'imageFilename': p['imageFilename'],
                        'is_ton_prize': p['is_ton_prize']
                    })
        return normalized_prizes

    filler_prize_candidate = None
    min_value = Decimal('inf')
    
    for p in prizes:
        if p['floor_price'] > 0:
            if p['floor_price'] < min_value:
                min_value = p['floor_price']
                filler_prize_candidate = p
            elif p['floor_price'] == min_value and (filler_prize_candidate is None or p['probability'] > filler_prize_candidate['probability']):
                filler_prize_candidate = p

    if not filler_prize_candidate or filler_prize_candidate['floor_price'] == Decimal('0') or len(prizes) < 2:
        logger.warning(f"No suitable filler prize found for case {case_data['id']} or filler has 0 value or too few prizes. Falling back to proportional scaling for all prizes.")
        return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)

    filler_prize_idx = -1
    for i, p in enumerate(prizes):
        if p is filler_prize_candidate:
            filler_prize_idx = i
            break
            
    if filler_prize_idx == -1:
        logger.error(f"Internal error: Filler prize not found in the prize list for case {case_data['id']}. Falling back to proportional scaling.")
        return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)

    filler_prize = prizes[filler_prize_idx]
    
    sum_non_filler_ev = Decimal('0')
    non_filler_total_initial_prob = Decimal('0')

    for p in prizes:
        if p is not filler_prize:
            sum_non_filler_ev += p['floor_price'] * p['probability']
            non_filler_total_initial_prob += p['probability']

    remaining_ev_for_filler = target_ev - sum_non_filler_ev
    
    if filler_prize['floor_price'] == Decimal('0'):
        logger.error(f"Filler prize for case {case_data['id']} has 0 floor price during calculation. Using proportional scaling.")
        return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)

    required_filler_prob = remaining_ev_for_filler / filler_prize['floor_price']

    if not (Decimal('0') <= required_filler_prob <= Decimal('1')):
        logger.warning(f"Required filler prob for {case_data['id']} out of bounds ({required_filler_prob}). Falling back to proportional scaling.")
        return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)

    if non_filler_total_initial_prob > 0:
        scale_others_factor = (Decimal('1.0') - required_filler_prob) / non_filler_total_initial_prob
        if scale_others_factor < Decimal('0') or not math.isfinite(float(scale_others_factor)):
            logger.warning(f"Scale factor for non-filler items for {case_data['id']} is invalid ({scale_others_factor}). Falling back to proportional scaling.")
            return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
        for p in prizes:
            if p is not filler_prize:
                p['probability'] *= scale_others_factor
    else:
        required_filler_prob = Decimal('1.0')

    filler_prize['probability'] = required_filler_prob

    current_sum_probs = sum(p['probability'] for p in prizes)
    if abs(current_sum_probs - Decimal('1.0')) > Decimal('1E-7'):
        diff = Decimal('1.0') - current_sum_probs
        if prizes:
            prizes[0]['probability'] += diff

    return [{
        'name': p['name'],
        'probability': float(p['probability'].quantize(Decimal('1E-7'))),
        'floor_price': float(p['floor_price']),
        'imageFilename': p['imageFilename'], # Preserved
        'is_ton_prize': p['is_ton_prize'] # Preserved
    } for p in prizes]

def calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices):
    """
    Fallback function for RTP calculation: proportionally scales all probabilities.
    Ensures that the sum of probabilities is 1.0 and total EV matches target EV.
    """
    case_price = Decimal(str(case_data['priceTON']))
    target_ev = case_price * RTP_TARGET

    prizes = []
    for p_info in case_data['prizes']:
        prize_name = p_info['name']
        floor_price = Decimal(str(all_floor_prices.get(prize_name, 0)))
        image_filename = p_info.get('imageFilename', generate_image_filename_from_name(prize_name))
        is_ton_prize = p_info.get('is_ton_prize', False)
        prizes.append({'name': prize_name, 'probability': Decimal(str(p_info['probability'])), 'floor_price': floor_price, 'imageFilename': image_filename, 'is_ton_prize': is_ton_prize})

    current_total_ev = sum(p['floor_price'] * p['probability'] for p in prizes)
    
    if current_total_ev == Decimal('0'):
        logger.warning(f"Proportional fallback for {case_data['id']}: Current total EV is zero. Normalizing probabilities without EV adjustment.")
        total_original_prob = sum(p['probability'] for p in prizes)
        normalized_prizes = []
        if total_original_prob > 0:
            for p in prizes:
                normalized_prizes.append({
                    'name': p['name'],
                    'probability': float((p['probability'] / total_original_prob).quantize(Decimal('1E-7'))),
                    'floor_price': float(p['floor_price']),
                    'imageFilename': p['imageFilename'],
                    'is_ton_prize': p['is_ton_prize']
                })
        else:
            if prizes:
                equal_prob = Decimal('1.0') / len(prizes)
                for p in prizes:
                    normalized_prizes.append({
                        'name': p['name'],
                        'probability': float(equal_prob.quantize(Decimal('1E-7'))),
                        'floor_price': float(p['floor_price']),
                        'imageFilename': p['imageFilename'],
                        'is_ton_prize': p['is_ton_prize']
                    })
        return normalized_prizes

    scale_factor = target_ev / current_total_ev
    
    for p in prizes:
        p['probability'] = p['probability'] * scale_factor
    
    total_prob_after_scaling = sum(p['probability'] for p in prizes)
    if total_prob_after_scaling == Decimal('0'):
        logger.error(f"Proportional fallback for {case_data['id']}: Total probability after scaling is zero. Cannot normalize.")
        return []

    for p in prizes:
        p['probability'] = p['probability'] / total_prob_after_scaling

    final_total_prob = sum(p['probability'] for p in prizes)
    if abs(final_total_prob - Decimal('1.0')) > Decimal('1E-7'):
        diff = Decimal('1.0') - final_total_prob
        if prizes:
            prizes[0]['probability'] += diff

    return [{
        'name': p['name'],
        'probability': float(p['probability'].quantize(Decimal('1E-7'))),
        'floor_price': float(p['floor_price']),
        'imageFilename': p['imageFilename'], # Preserved
        'is_ton_prize': p['is_ton_prize'] # Preserved
    } for p in prizes]


def calculate_rtp_probabilities_for_slots(slot_data, all_floor_prices):
    """
    Calculates and adjusts prize probabilities for a given slot data
    to achieve a target RTP, considering slot-specific EV calculation (multi-reel matching).
    """
    slot_price = Decimal(str(slot_data['priceTON']))
    target_ev = slot_price * RTP_TARGET
    num_reels = Decimal(str(slot_data.get('reels_config', 3)))

    prizes = []
    for p_info in slot_data['prize_pool']:
        prize_name = p_info['name']
        value_source = p_info.get('value', p_info.get('floorPrice', 0))
        floor_price = Decimal(str(value_source))
        image_filename = p_info.get('imageFilename', generate_image_filename_from_name(prize_name)) # Preserve image filename
        is_ton_prize = p_info.get('is_ton_prize', False) # Preserve is_ton_prize
        prizes.append({
            'name': prize_name,
            'probability': Decimal(str(p_info['probability'])),
            'floor_price': floor_price,
            'imageFilename': image_filename, # Preserved
            'is_ton_prize': is_ton_prize # Preserved
        })

    current_total_ev = Decimal('0')
    for p in prizes:
        if p['is_ton_prize']:
            current_total_ev += p['probability'] * p['floor_price'] * num_reels
        else:
            current_total_ev += (p['probability'] ** num_reels) * p['floor_price']

    if current_total_ev == Decimal('0'):
        logger.warning(f"Slot {slot_data['id']}: Current total EV is zero. Normalizing probabilities without EV adjustment.")
        total_original_prob = sum(p['probability'] for p in prizes)
        normalized_prizes = []
        if total_original_prob > 0:
            for p in prizes:
                normalized_prizes.append({
                    'name': p['name'],
                    'probability': float((p['probability'] / total_original_prob).quantize(Decimal('1E-7'))),
                    'floor_price': float(p['floor_price']),
                    'imageFilename': p['imageFilename'], # Preserved
                    'is_ton_prize': p['is_ton_prize'] # Preserved
                })
        else:
            if prizes:
                equal_prob = Decimal('1.0') / len(prizes)
                for p in prizes:
                    normalized_prizes.append({
                        'name': p['name'],
                        'probability': float(equal_prob.quantize(Decimal('1E-7'))),
                        'floor_price': float(p['floor_price']),
                        'imageFilename': p['imageFilename'], # Preserved
                        'is_ton_prize': p['is_ton_prize'] # Preserved
                    })
        return normalized_prizes

    scale_factor = target_ev / current_total_ev
    
    for p in prizes:
        p['probability'] *= scale_factor
    
    total_prob_after_scaling = sum(p['probability'] for p in prizes)
    if total_prob_after_scaling == Decimal('0'):
        logger.error(f"Slot {slot_data['id']}: Total probability after scaling is zero. Cannot normalize.")
        return []

    for p in prizes:
        p['probability'] /= total_prob_after_scaling

    final_total_prob = sum(p['probability'] for p in prizes)
    if abs(final_total_prob - Decimal('1.0')) > Decimal('1E-7'):
        diff = Decimal('1.0') - final_total_prob
        if prizes:
            prizes[0]['probability'] += diff

    return [{
        'name': p['name'],
        'probability': float(p['probability'].quantize(Decimal('1E-7'))),
        'floor_price': float(p['floor_price']),
        'imageFilename': p['imageFilename'], # Preserved
        'is_ton_prize': p['is_ton_prize'] # Preserved
    } for p in prizes]


# --- Game Data (Cases and Slots) ---

# Kissed Frog Prize Pool (initial template - will be adjusted by RTP function)

finalKissedFrogPrizesWithConsolation_Python = sorted([
    # Extremely rare expensive frogs
    {'name': 'Happy Pepe', 'probability': 0.0000001},  # Was 0.00010
    {'name': 'Tree Frog', 'probability': 0.0000005},  # Was 0.00050
    {'name': 'Brewtoad', 'probability': 0.0000005},
    {'name': 'Puddles', 'probability': 0.0000005},
    {'name': 'Honeyhop', 'probability': 0.0000005},
    {'name': 'Melty Butter', 'probability': 0.0000005},
    {'name': 'Lucifrog', 'probability': 0.0000005},
    {'name': 'Zodiak Croak', 'probability': 0.0000005},
    {'name': 'Count Croakula', 'probability': 0.0000005},
    {'name': 'Lilie Pond', 'probability': 0.0000005}, # Original name, not Lily Pond for this tier
    {'name': 'Sweet Dream', 'probability': 0.0000005},
    {'name': 'Frogmaid', 'probability': 0.0000005},
    {'name': 'Rocky Hopper', 'probability': 0.0000005},

    # Rare expensive frogs
    {'name': 'Icefrog', 'probability': 0.000002},  # Was 0.0020
    {'name': 'Lava Leap', 'probability': 0.000002},
    {'name': 'Toadstool', 'probability': 0.000002},
    {'name': 'Desert Frog', 'probability': 0.000002},
    {'name': 'Cupid', 'probability': 0.000002},
    {'name': 'Hopberry', 'probability': 0.000002},
    {'name': 'Ms. Toad', 'probability': 0.000002},
    {'name': 'Trixie', 'probability': 0.000002},
    {'name': 'Prince Ribbit', 'probability': 0.000002},
    {'name': 'Pond Fairy', 'probability': 0.000002},
    {'name': 'Boingo', 'probability': 0.000002},
    {'name': 'Tesla Frog', 'probability': 0.000002},

    # Uncommon, still valuable but less than case price or slightly above (very low chance if above)
    {'name': 'Starry Night', 'probability': 0.00001},  # Was 0.0070, Price 30
    {'name': 'Silver', 'probability': 0.00001},        # Price 30
    {'name': 'Ectofrog', 'probability': 0.00001},      # Price 30
    {'name': 'Poison', 'probability': 0.00001},        # Price 30
    {'name': 'Minty Bloom', 'probability': 0.00001},   # Price 30
    {'name': 'Sarutoad', 'probability': 0.00001},      # Price 30
    {'name': 'Void Hopper', 'probability': 0.00001},   # Price 30
    {'name': 'Ramune', 'probability': 0.00001},        # Price 30
    {'name': 'Lemon Drop', 'probability': 0.00001},    # Price 30
    {'name': 'Ectobloom', 'probability': 0.00001},     # Price 30
    {'name': 'Duskhopper', 'probability': 0.00001},    # Price 30
    {'name': 'Bronze', 'probability': 0.00001},        # Price 30

    # Common consolation prizes (slightly higher chance than valuable ones, but still low overall)
    {'name': 'Lily Pond', 'probability': 0.001}, # Was 0.04028, Price 19
    {'name': 'Toadberry', 'probability': 0.001},
    {'name': 'Frogwave', 'probability': 0.001},
    {'name': 'Melon', 'probability': 0.001},
    {'name': 'Sky Leaper', 'probability': 0.001},
    {'name': 'Frogtart', 'probability': 0.001},
    {'name': 'Peach', 'probability': 0.001},
    {'name': 'Sea Breeze', 'probability': 0.001},
    {'name': 'Lemon Juice', 'probability': 0.001},
    {'name': 'Cranberry', 'probability': 0.001},
    {'name': 'Tide Pod', 'probability': 0.001},
    {'name': 'Brownie', 'probability': 0.001},
    {'name': 'Banana Pox', 'probability': 0.001}, # Was 0.04024

    # NEW: Spy Agaric (Floor price 2.8 TON) - relatively common filler
    {'name': 'Spy Agaric', 'probability': 0.20}, # High-ish initial weight

    # Desk Calendar (Floor price 1.1/1.4 TON) - VERY common filler
    {'name': 'Desk Calendar', 'probability': 0.78} # Very high initial weight
], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)

full_finalKissedFrogPrizesWithConsolation_js = [
    {'name':'Happy Pepe','probability':0.0000850,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Happy%20Pepe.png','floorPrice':500.0},
    {'name':'Tree Frog','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Tree%20Frog.png','floorPrice':150.0},
    {'name':'Brewtoad','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Brewtoad.png','floorPrice':150.0},
    {'name':'Puddles','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Puddles.png','floorPrice':150.0},
    {'name':'Honeyhop','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Honeyhop.png','floorPrice':150.0},
    {'name':'Melty Butter','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Melty%20Butter.png','floorPrice':150.0},
    {'name':'Lucifrog','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Lucifrog.png','floorPrice':150.0},
    {'name':'Zodiak Croak','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Zodiak%20Croak.png','floorPrice':150.0},
    {'name':'Count Croakula','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Count%20Croakula.png','floorPrice':150.0},
    {'name':'Lilie Pond','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Lilie%20Pond.png','floorPrice':150.0},
    {'name':'Sweet Dream','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Sweet%20Dream.png','floorPrice':150.0},
    {'name':'Frogmaid','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Frogmaid.png','floorPrice':150.0},
    {'name':'Rocky Hopper','probability':0.0004250,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Rocky%20Hopper.png','floorPrice':150.0},
    {'name':'Icefrog','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Icefrog.png','floorPrice':45.0},
    {'name':'Lava Leap','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Lava%20Leap.png','floorPrice':45.0},
    {'name':'Toadstool','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Toadstool.png','floorPrice':45.0},
    {'name':'Desert Frog','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Desert%20Frog.png','floorPrice':45.0},
    {'name':'Cupid','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Cupid.png','floorPrice':45.0},
    {'name':'Hopberry','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Hopberry.png','floorPrice':45.0},
    {'name':'Ms. Toad','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Ms.%20Toad.png','floorPrice':45.0},
    {'name':'Trixie','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Trixie.png','floorPrice':45.0},
    {'name':'Prince Ribbit','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Prince%20Ribbit.png','floorPrice':45.0},
    {'name':'Pond Fairy','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Pond%20Fairy.png','floorPrice':45.0},
    {'name':'Boingo','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Boingo.png','floorPrice':45.0},
    {'name':'Tesla Frog','probability':0.0017000,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Tesla%20Frog.png','floorPrice':45.0},
    {'name':'Starry Night','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Starry%20Night.png','floorPrice':30.0},
    {'name':'Silver','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Silver.png','floorPrice':30.0},
    {'name':'Ectofrog','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Ectofrog.png','floorPrice':30.0},
    {'name':'Poison','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Poison.png','floorPrice':30.0},
    {'name':'Minty Bloom','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Minty%20Bloom.png','floorPrice':30.0},
    {'name':'Sarutoad','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Sarutoad.png','floorPrice':30.0},
    {'name':'Void Hopper','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Void%20Hopper.png','floorPrice':30.0},
    {'name':'Ramune','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Ramune.png','floorPrice':30.0},
    {'name':'Lemon Drop','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Lemon%20Drop.png','floorPrice':30.0},
    {'name':'Ectobloom','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Ectobloom.png','floorPrice':30.0},
    {'name':'Duskhopper','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Duskhopper.png','floorPrice':30.0},
    {'name':'Bronze','probability':0.0059500,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Bronze.png','floorPrice':30.0},
    {'name':'Lily Pond','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Lily%20Pond.png','floorPrice':19.0},
    {'name':'Toadberry','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Toadberry.png','floorPrice':19.0},
    {'name':'Frogwave','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Frogwave.png','floorPrice':19.0},
    {'name':'Melon','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Melon.png','floorPrice':19.0},
    {'name':'Sky Leaper','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Sky%20Leaper.png','floorPrice':19.0},
    {'name':'Frogtart','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Frogtart.png','floorPrice':19.0},
    {'name':'Peach','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Peach.png','floorPrice':19.0},
    {'name':'Sea Breeze','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Sea%20Breeze.png','floorPrice':19.0},
    {'name':'Lemon Juice','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Lemon%20Juice.png','floorPrice':19.0},
    {'name':'Cranberry','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Cranberry.png','floorPrice':19.0},
    {'name':'Tide Pod','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Tide%20Pod.png','floorPrice':19.0},
    {'name':'Brownie','probability':0.0342380,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Brownie.png','floorPrice':19.0},
    {'name':'Banana Pox','probability':0.0342340,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Banana%20Pox.png','floorPrice':19.0},
    {'name':'Desk Calendar','probability':0.0000000,'imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/GiftImages/Desk-Calendar.png','floorPrice':1.4}
]
kissed_frog_processed_prizes = sorted(
    full_finalKissedFrogPrizesWithConsolation_js, # Use the full list from your JS
    key=lambda p: p.get('floorPrice', 0),
    reverse=True
)

# Apply RTP calculation to Kissed Frog prizes now to get its final probabilities
kissed_frog_processed_prizes = calculate_rtp_probabilities(
    {'id':'kissedfrog','name':'Kissed Frog Pond','priceTON':20.0,'prizes':finalKissedFrogPrizesWithConsolation_Python},
    UPDATED_FLOOR_PRICES
)

# Backend cases data (initial templates - will be adjusted by RTP function)
# --- In app.py, REPLACE the entire cases_data_backend_with_fixed_prices_raw list ---
cases_data_backend_with_fixed_prices_raw = [
    {'id':'all_in_01','name':'All In','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/All-In.jpg','priceTON':0.2,'prizes': sorted([
        {'name':'Heart Locket','probability': 0.0000001}, # Jackpot!
        {'name':'Plush Pepe','probability': 0.0000005},
        {'name':'Durov\'s Cap','probability': 0.000005},
        {'name':'Precious Peach','probability': 0.00002},
        {'name':'Whip Cupcake', 'probability': 0.001}, # New common filler
        {'name':'Lol Pop','probability': 0.001},
        {'name': "Heart",  'probability': 0.25},
        {'name': "Bear",   'probability': 0.25},
        {'name': "Rose",   'probability': 0.20},
        {'name': "Rocket", 'probability': 0.1389734},
        {'name': "Bottle", 'probability': 0.15},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id':'small_billionaire_05','name':'Small Billionaire','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Small-Billionaire.jpg','priceTON':0.756,'prizes': sorted([
        {'name':'Heroic Helmet','probability': 0.000005}, # New high-tier prize
        {'name':'Perfume Bottle','probability': 0.0001},
        {'name':'Vintage Cigar','probability': 0.00012},
        {'name':'Signet Ring','probability': 0.00013},
        {'name':'Swiss Watch','probability': 0.00015},
        {'name':'Holiday Drink', 'probability': 0.002},  # New mid-tier filler
        {'name':'Swag Bag', 'probability': 0.002},      # New mid-tier filler
        {'name':'Snake Box', 'probability': 0.005},
        {'name': "Heart",  'probability': 0.25},
        {'name': "Bear",   'probability': 0.25},
        {'name': "Rose",   'probability': 0.20},
        {'name': "Rocket", 'probability': 0.140495},
        {'name': "Bottle", 'probability': 0.15},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id':'lolpop','name':'Lol Pop Stash','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Lol-Pop.jpg','priceTON':2.8,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.0000001},
        {'name':'Neko Helmet','probability':0.00001},
        {'name':'Snoop Dogg', 'probability': 0.05},      # New Thematic Filler
        {'name':'Whip Cupcake', 'probability': 0.05},   # New Thematic Filler
        {'name':'Holiday Drink', 'probability': 0.05},  # New Thematic Filler
        {'name':'Swag Bag', 'probability': 0.05},      # New Thematic Filler
        {'name':'Snake Box', 'probability': 0.0005},
        {'name':'Pet Snake', 'probability': 0.0005},
        {'name':'Party Sparkler','probability':0.1},
        {'name':'Homemade Cake','probability':0.1},
        {'name':'Jack-in-the-box','probability':0.1},
        {'name':'Santa Hat','probability':0.1},
        {'name':'Jester Hat','probability':0.05},
        {'name':'Cookie Heart','probability':0.1},
        {'name':'Easter Egg','probability':0.05},
        {'name':'Lol Pop','probability':0.0988899},
        {'name':'Hypno Lollipop','probability':0.1},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id':'recordplayer','name':'Record Player Vault','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Record-Player.jpg','priceTON':3.6,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.0000001},
        {'name':'Bow Tie', 'probability': 0.02},         # New
        {'name':'Joyful Bundle', 'probability': 0.02},   # New
        {'name':'Lush Bouquet', 'probability': 0.02},    # New
        {'name':'Restless Jar', 'probability': 0.02},    # New
        {'name':'Light Sword', 'probability': 0.02},     # New
        {'name':'Tama Gadget','probability':0.001},
        {'name':'Record Player','probability':0.001},
        {'name':'Big Year', 'probability': 0.001},
        {'name':'Flying Broom','probability':0.001},
        {'name':'Skull Flower','probability':0.001},
        {'name':'Pet Snake', 'probability': 0.05},
        {'name':'Hex Pot','probability':0.1},
        {'name':'Snow Mittens','probability':0.1},
        {'name':'Spy Agaric','probability':0.0809999},
        {'name':'Star Notepad','probability':0.1},
        {'name':'Ginger Cookie','probability':0.1},
        {'name':'Party Sparkler','probability':0.15},
        {'name':'Lol Pop','probability':0.15},
        {'name':'Hypno Lollipop','probability':0.065},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id': 'girls_collection', 'name': 'Girl\'s Collection', 'imageFilename': 'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/girls.jpg', 'priceTON': 8.0, 'prizes': sorted([
        {'name':'Heart Locket', 'probability': 0.00005},  # New Ultimate Prize
        {'name':'Nail Bracelet', 'probability': 0.0001}, # New Top Prize
        {'name':'Loot Bag', 'probability': 0.00001},
        {'name':'Genie Lamp', 'probability': 0.00001},
        {'name':'Cupid Charm', 'probability': 0.1},      # New Thematic Prize
        {'name':'Valentine Box', 'probability': 0.1},    # New Thematic Prize
        {'name':'Lush Bouquet', 'probability': 0.1},     # New Thematic Prize
        {'name':'Eternal Rose', 'probability': 0.1},
        {'name': 'Berry Box', 'probability': 0.2},
        {'name': 'Sakura Flower', 'probability': 0.2},
        {'name': 'Bunny Muffin', 'probability': 0.19983},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id': 'mens_collection', 'name': 'Men\'s Collection', 'imageFilename': 'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/men.jpg', 'priceTON': 8.0, 'prizes': sorted([
        {'name':'Heroic Helmet', 'probability': 0.0001}, # New Top Prize
        {'name':'Durov\'s Cap', 'probability': 0.000001},
        {'name':'Westside Sign', 'probability': 0.05},   # New Snoop Dogg Set
        {'name':'Low Rider', 'probability': 0.1},        # New Snoop Dogg Set
        {'name':'Snoop Cigar', 'probability': 0.15},     # New Snoop Dogg Set
        {'name':'Swag Bag', 'probability': 0.2},         # New Snoop Dogg Set
        {'name':'Snoop Dogg', 'probability': 0.2},       # New Snoop Dogg Set
        {'name':'Vintage Cigar', 'probability': 0.0001},
        {'name':'Signet Ring', 'probability': 0.0001},
        {'name':'Top Hat', 'probability': 0.1},
        {'name':'Record Player', 'probability': 0.1},
        {'name':'Spiced Wine', 'probability': 0.099699},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id':'swisswatch','name':'Swiss Watch Box','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Swiss-Watch.jpg','priceTON':10.0,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.0000001},
        {'name':'Low Rider', 'probability': 0.02},       # New
        {'name':'Cupid Charm', 'probability': 0.05},     # New
        {'name':'Valentine Box', 'probability': 0.05},   # New
        {'name':'Snoop Cigar', 'probability': 0.05},     # New
        {'name':'Signet Ring','probability':0.00001},
        {'name':'Swiss Watch','probability':0.00001},
        {'name':'Electric Skull','probability':0.0001},
        {'name':'Voodoo Doll','probability':0.1},
        {'name':'Diamond Ring','probability':0.1},
        {'name':'Love Candle','probability':0.1},
        {'name':'Mad Pumpkin','probability':0.0798799},
        {'name':'Sleigh Bell','probability':0.1},
        {'name':'Top Hat','probability':0.1},
        {'name':'Trapped Heart','probability':0.1},
        {'name':'Love Potion','probability':0.1},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id':'kissedfrog','name':'Kissed Frog Pond','priceTON':20.0,'imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Kissed-Frog.jpg',
     'prizes': finalKissedFrogPrizesWithConsolation_Python # Keeping this case pure to its theme
    },

    {'id':'perfumebottle','name':'Perfume Chest','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Perfume-Bottle.jpg','priceTON': 20.0,'prizes': sorted([
        {'name':'Heart Locket','probability':0.000001},
        {'name':'Nail Bracelet', 'probability': 0.0005},
        {'name':'Westside Sign', 'probability': 0.02},
        {'name':'Low Rider', 'probability': 0.02},
        {'name':'Plush Pepe','probability':0.0000001},
        {'name':'Bonded Ring', 'probability': 0.0000005},
        {'name':'Perfume Bottle','probability':0.000005},
        {'name':'Magic Potion','probability':0.00001},
        {'name':'Genie Lamp','probability':0.01},
        {'name':'Swiss Watch','probability':0.01},
        {'name':'Sharp Tongue','probability':0.02},
        {'name':'Neko Helmet','probability':0.02},
        {'name':'Kissed Frog','probability':0.05},
        {'name':'Electric Skull','probability':0.1},
        {'name':'Diamond Ring','probability':0.1},
        {'name':'Big Year', 'probability': 0.0894834},
        {'name':'Snake Box', 'probability': 0.5},
        {'name':'Pet Snake', 'probability': 0.05},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id':'vintagecigar','name':'Vintage Cigar Safe','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Vintage-Cigar.jpg','priceTON':40.0,'prizes': sorted([
        {'name':'Heart Locket','probability':0.000005},
        {'name':'Heroic Helmet','probability':0.00005},
        {'name':'Nail Bracelet','probability':0.0001},
        {'name':'Gem Signet','probability':0.05},
        {'name':'Westside Sign','probability':0.05},
        {'name':'Plush Pepe','probability':0.0000001},
        {'name':'Mini Oscar','probability':0.00001},
        {'name':'Perfume Bottle','probability':0.01},
        {'name':'Scared Cat','probability':0.1},
        {'name':'Vintage Cigar','probability':0.1},
        {'name':'Swiss Watch','probability':0.05},
        {'name':'Sharp Tongue','probability':0.0898349},
        {'name':'Genie Lamp','probability':0.1},
        {'name':'Toy Bear','probability':0.15},
        {'name':'Neko Helmet','probability':0.1},
        {'name':'Snake Box', 'probability': 0.1},
        {'name':'Pet Snake', 'probability': 0.05},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id':'astralshard','name':'Astral Shard Relic','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Astral-Shard.jpg','priceTON':100.0,'prizes': sorted([
        {'name':'Heart Locket','probability':0.0001},
        {'name':'Heroic Helmet','probability':0.05},
        {'name':'Nail Bracelet','probability':0.1},
        {'name':'Gem Signet','probability':0.1},
        {'name':'Plush Pepe','probability':0.0000001},
        {'name':'Durov\'s Cap','probability':0.0000005},
        {'name':'Precious Peach','probability':0.000001},
        {'name':'Bonded Ring', 'probability': 0.01},
        {'name':'Astral Shard','probability':0.05},
        {'name':'Ion Gem','probability':0.05},
        {'name':'Mini Oscar','probability':0.05},
        {'name':'Perfume Bottle','probability':0.05},
        {'name':'Magic Potion','probability':0.05},
        {'name':'Loot Bag','probability':0.0898984},
        {'name':'Scared Cat','probability':0.1},
        {'name':'Vintage Cigar','probability':0.1},
        {'name':'Swiss Watch','probability':0.1},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},

    {'id':'plushpepe','name':'Plush Pepe Hoard','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Plush-Pepe.jpg','priceTON': 200.0,'prizes': sorted([
        {'name':'Heart Locket','probability':0.05},
        {'name':'Heroic Helmet','probability':0.1},
        {'name':'Nail Bracelet','probability':0.1},
        {'name':'Plush Pepe','probability':0.000001},
        {'name':'Durov\'s Cap','probability':0.000005},
        {'name':'Precious Peach','probability':0.4},
        {'name':'Bonded Ring', 'probability': 0.2},
        {'name':'Astral Shard','probability': 0.149994},
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)}
]

cases_data_backend = []
for case_template in cases_data_backend_with_fixed_prices_raw:
    processed_case = {**case_template}
    try:
        processed_case['prizes'] = calculate_rtp_probabilities(processed_case, UPDATED_FLOOR_PRICES)
        cases_data_backend.append(processed_case)
    except Exception as e:
        # Log the error and skip this case if RTP calculation fails
        case_id = case_template.get('id', 'N/A')
        case_name = case_template.get('name', 'Unnamed Case')
        logger.error(f"Failed to process case '{case_name}' (ID: {case_id}) for RTP. Skipping this case. Error: {e}", exc_info=True)
        # This will cause the case to be 'not found' by the API if requested.
        # You might want to add a dummy case or a specific error message if this happens frequently.

DEFAULT_SLOT_TON_PRIZES = [
    {'name': "0.1 TON", 'value': 0.1, 'is_ton_prize': True, 'probability': 0.1},
    {'name': "0.25 TON", 'value': 0.25, 'is_ton_prize': True, 'probability': 0.08},
    {'name': "0.5 TON", 'value': 0.5, 'is_ton_prize': True, 'probability': 0.05}
]
PREMIUM_SLOT_TON_PRIZES = [
    {'name': "2 TON", 'value': 2.0, 'is_ton_prize': True, 'probability': 0.08},
    {'name': "3 TON", 'value': 3.0, 'is_ton_prize': True, 'probability': 0.05},
    {'name': "5 TON", 'value': 5.0, 'is_ton_prize': True, 'probability': 0.03}
]

ALL_ITEMS_POOL_FOR_SLOTS = [{'name': name, 'floorPrice': price, 'imageFilename': generate_image_filename_from_name(name), 'is_ton_prize': False}
                            for name, price in UPDATED_FLOOR_PRICES.items()]

slots_data_backend = []

for name, data in EMOJI_GIFTS_BACKEND.items():
    UPDATED_FLOOR_PRICES[name] = data['value'] / TON_TO_STARS_RATE_BACKEND

# Replace "Nothing" prize in cases data
for case in cases_data_backend_with_fixed_prices_raw:
    # Find and remove 'Nothing' prize if it exists
    case['prizes'] = [p for p in case['prizes'] if p['name'] != 'Nothing']
    # Add new emoji gifts as low-tier prizes
    case['prizes'].extend([
        {'name': "💝", 'probability': 0.25},
        {'name': "🐻", 'probability': 0.25},
        {'name': "🌹", 'probability': 0.20},
        {'name': "🚀", 'probability': 0.15},
        {'name': "🍾", 'probability': 0.15},
    ])

def finalize_slot_prize_pools():
    global slots_data_backend
    updated_slots_data_backend = []
    
    default_slot_prizes_template = []
    default_slot_prizes_template.extend([
        {'name': "0.1 TON", 'value': 0.1, 'is_ton_prize': True, 'probability': 0.1},
        {'name': "0.25 TON", 'value': 0.25, 'is_ton_prize': True, 'probability': 0.08},
        {'name': "0.5 TON", 'value': 0.5, 'is_ton_prize': True, 'probability': 0.05}
    ])
    item_candidates_default = [item for item in ALL_ITEMS_POOL_FOR_SLOTS if item['floorPrice'] <= 5.0 and not item.get('is_ton_prize') and item['name'] not in [p['name'] for p in default_slot_prizes_template if not p.get('is_ton_prize')]]
    for item in item_candidates_default:
        default_slot_prizes_template.append({
            'name': item['name'],
            'imageFilename': item['imageFilename'],
            'floorPrice': item['floorPrice'],
            'is_ton_prize': False,
            'probability': 0.01
        })
    if len(default_slot_prizes_template) < 10:
        default_slot_prizes_template.append({'name':'Desk Calendar', 'floorPrice':UPDATED_FLOOR_PRICES['Desk Calendar'], 'probability':0.001})

    default_slot_data = { 'id': 'default_slot', 'name': 'Default Slot', 'priceTON': 3.0, 'reels_config': 3, 'prize_pool': default_slot_prizes_template }
    default_slot_data['prize_pool'] = calculate_rtp_probabilities_for_slots(default_slot_data, UPDATED_FLOOR_PRICES)
    updated_slots_data_backend.append(default_slot_data)

    premium_slot_prizes_template = []
    premium_slot_prizes_template.extend([
        {'name': "2 TON", 'value': 2.0, 'is_ton_prize': True, 'probability': 0.08},
        {'name': "3 TON", 'value': 3.0, 'is_ton_prize': True, 'probability': 0.05},
        {'name': "5 TON", 'value': 5.0, 'is_ton_prize': True, 'probability': 0.03}
    ])
    item_candidates_premium = [item for item in ALL_ITEMS_POOL_FOR_SLOTS if item['floorPrice'] > 5.0 and not item.get('is_ton_prize') and item['name'] not in [p['name'] for p in premium_slot_prizes_template if not p.get('is_ton_prize')]]
    for item in item_candidates_premium:
        premium_slot_prizes_template.append({
            'name': item['name'],
            'imageFilename': item['imageFilename'],
            'floorPrice': item['floorPrice'],
            'is_ton_prize': False,
            'probability': 0.005
        })

    premium_slot_data = { 'id': 'premium_slot', 'name': 'Premium Slot', 'priceTON': 10.0, 'reels_config': 3, 'prize_pool': premium_slot_prizes_template }
    premium_slot_data['prize_pool'] = calculate_rtp_probabilities_for_slots(premium_slot_data, UPDATED_FLOOR_PRICES)
    updated_slots_data_backend.append(premium_slot_data)

    slots_data_backend = updated_slots_data_backend

finalize_slot_prize_pools()


def calculate_and_log_rtp():
    logger.info("--- RTP Calculations (Based on Current Fixed Prices & Probabilities) ---")
    overall_total_ev_weighted_by_price = Decimal('0')
    overall_total_cost_sum = Decimal('0')

    all_games_data = cases_data_backend + slots_data_backend

    for game_data in all_games_data:
        game_id = game_data['id']
        game_name = game_data['name']
        price = Decimal(str(game_data['priceTON']))
        
        current_ev = Decimal('0')

        if 'prizes' in game_data:
            for prize in game_data['prizes']:
                prize_value = Decimal(str(UPDATED_FLOOR_PRICES.get(prize['name'], 0)))
                current_ev += prize_value * Decimal(str(prize['probability']))
        elif 'prize_pool' in game_data:
            num_reels = Decimal(str(game_data.get('reels_config', 3)))
            for prize_spec in game_data['prize_pool']:
                value = Decimal(str(prize_spec.get('value', prize_spec.get('floorPrice', 0))))
                prob_on_reel = Decimal(str(prize_spec.get('probability', 0)))

                if prize_spec.get('is_ton_prize'):
                    current_ev += prob_on_reel * value * num_reels
                else:
                    current_ev += (prob_on_reel ** num_reels) * value
        
        rtp = (current_ev / price) * 100 if price > 0 else Decimal('0')
        dev_cut = 100 - rtp if price > 0 else Decimal('0')
        
        logger.info(f"Game: {game_name:<25} | Price: {price:>6.2f} TON | Est.EV: {current_ev:>6.2f} | Est.RTP: {rtp:>6.2f}% | Est.DevCut: {dev_cut:>6.2f}%")
        
        if price > 0:
            overall_total_ev_weighted_by_price += current_ev * price
            overall_total_cost_sum += price

    if overall_total_cost_sum > 0:
        weighted_avg_rtp = (overall_total_ev_weighted_by_price / overall_total_cost_sum) * 100
        logger.info(f"--- Approx. Weighted Avg RTP (by price, for priced games): {weighted_avg_rtp:.2f}% ---")
    else:
        logger.info("--- No priced games for overall RTP calculation. ---")


# --- Initial Data Population and Setup ---
def populate_initial_data():
    db = SessionLocal()
    try:
        for nft_name, floor_price in UPDATED_FLOOR_PRICES.items():
            nft_exists = db.query(NFT).filter(NFT.name == nft_name).first()
            img_filename_or_url = generate_image_filename_from_name(nft_name)
            
            if not nft_exists:
                db.add(NFT(name=nft_name, image_filename=img_filename_or_url, floor_price=floor_price))
            elif nft_exists.floor_price != floor_price or nft_exists.image_filename != img_filename_or_url:
                nft_exists.floor_price = floor_price
                nft_exists.image_filename = img_filename_or_url
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error populating initial NFT data: {e}", exc_info=True)
    finally:
        db.close()

def initial_setup_and_logging():
    populate_initial_data()
    db = SessionLocal()
    try:
        if not db.query(PromoCode).filter(PromoCode.code_text == 'Grachev').first():
            db.add(PromoCode(code_text='Grachev', activations_left=10, ton_amount=100.0))
            db.commit()
            logger.info("Seeded 'Grachev' promocode.")
        else:
            logger.info("'Grachev' promocode already exists. Skipping seeding.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error seeding Grachev promocode: {e}", exc_info=True)
    finally:
        db.close()
    
    calculate_and_log_rtp()

initial_setup_and_logging()


# --- Flask App Setup ---
app = Flask(__name__)
PROD_ORIGIN = "https://vasiliy-katsyka.github.io"
NULL_ORIGIN = "null"
LOCAL_DEV_ORIGINS = ["http://localhost:5500","http://127.0.0.1:5500","http://localhost:8000","http://127.0.0.1:8000",]
final_allowed_origins = list(set([PROD_ORIGIN, NULL_ORIGIN] + LOCAL_DEV_ORIGINS))
CORS(app, resources={r"/api/*": {"origins": final_allowed_origins}})
if BOT_TOKEN:
    setup_telegram_webhook(app)
else:
    logger.error("Cannot setup Telegram webhook because BOT_TOKEN is missing.")

# --- Database Session Helper ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- Telegram Mini App InitData Validation ---
def validate_init_data(init_data_str: str, bot_token_for_validation: str) -> dict | None:
    logger.debug(f"Attempting to validate initData: {init_data_str[:200]}...")
    try:
        if not init_data_str:
            logger.warning("validate_init_data: init_data_str is empty or None.")
            return None

        parsed_data = dict(parse_qs(init_data_str))
        
        for key, value_list in parsed_data.items():
            if value_list:
                parsed_data[key] = value_list[0]
            else:
                logger.warning(f"validate_init_data: Empty value list for key: {key}")
                return None

        required_keys = ['hash', 'user', 'auth_date']
        missing_keys = [k for k in required_keys if k not in parsed_data]
        if missing_keys:
            logger.warning(f"validate_init_data: Missing keys in parsed_data: {missing_keys}. Parsed: {list(parsed_data.keys())}")
            return None

        hash_received = parsed_data.pop('hash')
        auth_date_ts = int(parsed_data['auth_date'])
        current_ts = int(dt.now(timezone.utc).timestamp())

        if (current_ts - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS:
            logger.warning(f"validate_init_data: auth_date expired. auth_date_ts: {auth_date_ts}, current_ts: {current_ts}, diff: {current_ts - auth_date_ts}s, max_age: {AUTH_DATE_MAX_AGE_SECONDS}s")
            return None

        data_check_string_parts = []
        for k in sorted(parsed_data.keys()):
            if k == 'user':
                user_info_str_unquoted = unquote(parsed_data[k])
                data_check_string_parts.append(f"{k}={user_info_str_unquoted}")
            else:
                data_check_string_parts.append(f"{k}={parsed_data[k]}")
        
        data_check_string = "\n".join(data_check_string_parts)
        
        secret_key = hmac.new("WebAppData".encode(), bot_token_for_validation.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if calculated_hash_hex == hash_received:
            user_info_str_unquoted = unquote(parsed_data['user'])
            try:
                user_info_dict = json.loads(user_info_str_unquoted)
            except json.JSONDecodeError as je:
                logger.error(f"validate_init_data: Failed to parse user JSON: {user_info_str_unquoted}. Error: {je}")
                return None
            
            if 'id' not in user_info_dict:
                logger.warning(f"validate_init_data: 'id' not found in user_info_dict. User data: {user_info_dict}")
                return None
            
            user_info_dict['id'] = int(user_info_dict['id'])
            logger.info(f"validate_init_data: Hash matched for user ID: {user_info_dict.get('id')}. Auth successful.")
            return user_info_dict
        else:
            logger.warning(f"validate_init_data: Hash mismatch.")
            logger.debug(f"Received Hash: {hash_received}")
            logger.debug(f"Calculated Hash: {calculated_hash_hex}")
            logger.debug(f"Data Check String: {data_check_string[:500]}")
            logger.debug(f"BOT_TOKEN used for secret_key (first 5 chars): {bot_token_for_validation[:5]}...")
            return None
    except Exception as e_validate:
        logger.error(f"validate_init_data: General exception during initData validation: {e_validate}", exc_info=True)
        return None


# --- API Routes ---
@app.route('/')
def index_route():
    return "Case Hunter API Backend is Running!"

# --- Replace the existing check_subscription_api function ---

# Define your required channels at the top, near your other constants
REQUIRED_CHANNELS = ['@CompactTelegram', '@CaseHunterNews']

@app.route('/api/check_subscription', methods=['GET'])
def check_subscription_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    user_id = auth["id"]
    
    if not bot:
        # Fallback for development if the bot isn't configured
        return jsonify({"is_subscribed": True, "missing": []})

    missing_subscriptions = []
    for channel_id in REQUIRED_CHANNELS:
        try:
            chat_member = bot.get_chat_member(channel_id, user_id)
            if chat_member.status not in ['member', 'administrator', 'creator']:
                missing_subscriptions.append(channel_id)
        except Exception as e:
            # This error often means the user is not in the channel or the bot lacks permissions
            logger.warning(f"Could not verify subscription for user {user_id} in {channel_id}. Assuming not subscribed. Error: {e}")
            missing_subscriptions.append(channel_id)
            
    if not missing_subscriptions:
        # User is subscribed to all required channels
        return jsonify({"is_subscribed": True, "missing": []})
    else:
        # User is missing one or more subscriptions
        return jsonify({"is_subscribed": False, "missing": missing_subscriptions})

# --- In app.py ---


# ... (add near your other API routes) ...

@app.route('/api/internal/log_gift_deposit', methods=['POST'])
def log_gift_deposit_api():
    # SECURITY: A simple secret key check to ensure only our userbot can call this.
    # Set this secret key in your Render environment variables.
    USERBOT_SECRET_KEY = os.environ.get("USERBOT_SECRET_KEY")
    auth_header = flask_request.headers.get('X-Userbot-Secret')

    if not USERBOT_SECRET_KEY or auth_header != USERBOT_SECRET_KEY:
        logger.warning("Unauthorized attempt to access internal gift deposit API.")
        return jsonify({"error": "Unauthorized"}), 403

    data = flask_request.get_json()
    logger.info(f"Received gift deposit log request: {data}")

    try:
        from_user_id = int(data.get('from_user_id'))
        from_username = data.get('from_username', 'N/A')
        star_amount = int(data.get('star_amount'))
        gift_link = data.get('gift_link', 'N/A')
        gift_attributes = data.get('gift_attributes', 'N/A')

        if not all([from_user_id, star_amount]):
            return jsonify({"error": "Missing required data"}), 400

        # Send confirmation message to the admin/withdrawer
        if bot and TARGET_WITHDRAWER_ID:
            message_text = (
                f"🎁 *Новый Депозит Подарком*\n\n"
                f"От: {from_username} (ID: `{from_user_id}`)\n"
                f"Сумма: *{star_amount} Stars*\n"
                f"Подарок: {gift_link}\n\n"
                f"*Атрибуты:*\n{gift_attributes}\n\n"
                f"Добавить на баланс?"
            )
            
            # Create a unique payload for the callback buttons
            callback_payload = f"gift_deposit:{from_user_id}:{star_amount}"
            
            markup = types.InlineKeyboardMarkup()
            yes_button = types.InlineKeyboardButton("✅ Yes", callback_data=f"confirm_{callback_payload}")
            deny_button = types.InlineKeyboardButton("❌ Deny", callback_data=f"deny_{callback_payload}")
            markup.add(yes_button, deny_button)
            
            bot.send_message(TARGET_WITHDRAWER_ID, message_text, reply_markup=markup, parse_mode="Markdown")
            return jsonify({"status": "success", "message": "Admin notified for confirmation."})
        else:
            return jsonify({"error": "Bot or target withdrawer not configured."}), 500

    except (ValueError, TypeError) as e:
        logger.error(f"Invalid data in gift deposit log request: {e}")
        return jsonify({"error": "Invalid data format."}), 400
    except Exception as e:
        logger.error(f"Error in log_gift_deposit_api: {e}", exc_info=True)
        return jsonify({"error": "Internal server error."}), 500
        
@app.route('/api/healthcheck', methods=['GET'])
def health_check():
    """A simple endpoint to confirm the server is running."""
    logger.info("Health check endpoint was hit successfully.")
    return jsonify({"status": "ok", "message": "Server is running."}), 200

# ... (keep all existing code before this route) ...

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user:
            new_referral_code = f"ref_{uid}_{random.randint(1000,9999)}"
            while db.query(User).filter(User.referral_code == new_referral_code).first():
                new_referral_code = f"ref_{uid}_{random.randint(1000,9999)}"

            user = User(
                id=uid,
                username=auth.get("username"),
                first_name=auth.get("first_name"),
                last_name=auth.get("last_name"),
                referral_code=new_referral_code
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            logger.info(f"New user registered: {uid}")
        
        changed = False
        if user.username != auth.get("username"):
            user.username = auth.get("username")
            changed=True
        if user.first_name != auth.get("first_name"):
            user.first_name = auth.get("first_name")
            changed=True
        if user.last_name != auth.get("last_name"):
            user.last_name = auth.get("last_name")
            changed=True
        if changed:
            db.commit()
            db.refresh(user)

        inv = []
        for i in user.inventory:
            item_name = i.nft.name if i.nft else i.item_name_override
            
            # --- START OF THE FIX ---
            # Prioritize the canonical emoji image URL if the item is an emoji gift.
            # This corrects any old/incorrect image paths stored in the database.
            item_image = ""
            is_emoji = item_name in EMOJI_GIFT_IMAGES

            if is_emoji:
                item_image = EMOJI_GIFT_IMAGES[item_name]
            else:
                # Fallback to existing logic for all other items (NFTs, etc.)
                item_image = i.nft.image_filename if i.nft else i.item_image_override or generate_image_filename_from_name(item_name)
            # --- END OF THE FIX ---

            inv.append({
                "id":i.id,
                "name":item_name,
                "imageFilename":item_image, # This will now always be correct for emojis
                "floorPrice":i.nft.floor_price if i.nft else i.current_value,
                "currentValue":i.current_value,
                "upgradeMultiplier":i.upgrade_multiplier,
                "variant":i.variant,
                "is_ton_prize":i.is_ton_prize,
                "is_emoji_gift": is_emoji, # Add this flag for consistency
                "obtained_at":i.obtained_at.isoformat() if i.obtained_at else None
            })

        refs_count = db.query(User).filter(User.referred_by_id == uid).count()

        return jsonify({
            "id":user.id,
            "username":user.username,
            "first_name":user.first_name,
            "last_name":user.last_name,
            "tonBalance":user.ton_balance,
            "starBalance":user.star_balance,
            "inventory":inv,
            "referralCode":user.referral_code,
            "referralEarningsPending": int(user.referral_earnings_pending * TON_TO_STARS_RATE_BACKEND),
            "total_won_ton":user.total_won_ton,
            "invited_friends_count":refs_count
        })
    except Exception as e:
        logger.error(f"Error in get_user_data for {uid}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue."}), 500
    finally:
        db.close()

# ... (keep all existing code after this route) ...

@app.route('/api/get_invited_friends', methods=['GET'])
def get_invited_friends_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    db = next(get_db())
    try:
        # Find all users who were referred by the current user (uid)
        invited_friends = db.query(User).filter(User.referred_by_id == uid).order_by(User.created_at.desc()).all()
        
        friends_data = []
        for friend in invited_friends:
            display_name = friend.first_name or friend.username or f"User #{str(friend.id)[:6]}"
            friends_data.append({
                "id": friend.id,
                "name": display_name
                # You can add more data here if needed, e.g., friend.created_at
            })
            
        return jsonify(friends_data)
    except Exception as e:
        logger.error(f"Error in get_invited_friends for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "Could not load invited friends list."}), 500
    finally:
        db.close()

# --- Replace the entire register_referral_api function in your Python backend with this ---

# --- Find and REPLACE the entire register_referral_api function ---

# --- In app.py ---

# --- Find and REPLACE the entire register_referral_api function ---
@app.route('/api/register_referral', methods=['POST'])
def register_referral_api():
    data = flask_request.get_json()
    user_id = data.get('user_id')
    username = data.get('username')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    referral_code_used = data.get('referral_code')

    if not all([user_id, referral_code_used]):
        return jsonify({"error": "Missing user_id or referral_code"}), 400
    
    db = next(get_db())
    try:
        referrer = db.query(User).filter(User.referral_code == referral_code_used).with_for_update().first()
        if not referrer:
            db.commit()
            return jsonify({"error": "Referrer not found with this code."}), 404

        referred_user = db.query(User).filter(User.id == user_id).first()
        if not referred_user:
            new_referral_code_for_user = f"ref_{user_id}_{random.randint(1000,9999)}"
            while db.query(User).filter(User.referral_code == new_referral_code_for_user).first():
                new_referral_code_for_user = f"ref_{user_id}_{random.randint(1000,9999)}"

            referred_user = User(
                id=user_id, username=username, first_name=first_name,
                last_name=last_name, referral_code=new_referral_code_for_user
            )
            db.add(referred_user)
            db.flush()
        else:
            if referred_user.username != username: referred_user.username = username
            if referred_user.first_name != first_name: referred_user.first_name = first_name
            if referred_user.last_name != last_name: referred_user.last_name = last_name
        
        if referred_user.referred_by_id:
            db.commit()
            return jsonify({"status": "already_referred", "message": "User was already referred."}), 200
        
        if referrer.id == referred_user.id:
            db.commit()
            return jsonify({"error": "Cannot refer oneself."}), 400

        # --- THIS LOGIC IS ALREADY CORRECT - Adding bonus to PENDING earnings ---
        star_bonus = 5
        ton_equivalent_bonus = Decimal(str(star_bonus)) / Decimal(str(TON_TO_STARS_RATE_BACKEND))

        current_pending = Decimal(str(referrer.referral_earnings_pending))
        referrer.referral_earnings_pending = float(current_pending + ton_equivalent_bonus)
        
        referred_user.referred_by_id = referrer.id
        
        if bot:
            try:
                referral_rate_percent = int(DEFAULT_REFERRAL_RATE * 100)
                if referrer.username and referrer.username.lower() in (name.lower() for name in SPECIAL_REFERRAL_RATES.keys()):
                    special_rate = SPECIAL_REFERRAL_RATES[referrer.username.lower()]
                    referral_rate_percent = int(special_rate * 100)

                new_user_display_name = referred_user.first_name or referred_user.username or f"User #{str(referred_user.id)[:6]}"
                
                # The message correctly implies the bonus is part of the referral system, not direct balance.
                notification_message = (
                    f"🎉 *New Referral!* 🎉\n\n"
                    f"Your friend *{new_user_display_name}* has joined using your link. "
                    f"A *+{star_bonus} Stars* bonus has been added to your referral earnings!\n\n"
                    f"You will also earn *{referral_rate_percent}%* from their future deposits."
                )
                
                bot.send_message(chat_id=referrer.id, text=notification_message, parse_mode="Markdown")
                logger.info(f"Sent referral notification. Added +{ton_equivalent_bonus:.4f} TON to PENDING for referrer {referrer.id}.")

            except Exception as e_notify:
                logger.error(f"Failed to send referral notification to user {referrer.id}. Reason: {e_notify}")

        db.commit()
        logger.info(f"User {user_id} successfully referred by {referrer.id}. Referrer received +{ton_equivalent_bonus:.4f} TON in pending earnings.")
        
        return jsonify({"status": "success", "message": "Referral registered successfully."})
        
    except IntegrityError as ie:
        db.rollback()
        logger.error(f"Integrity error registering referral for {user_id} with code {referral_code_used}: {ie}", exc_info=True)
        return jsonify({"error": "Database integrity error, possibly concurrent registration."}), 409
    except Exception as e:
        db.rollback()
        logger.error(f"Error registering referral for {user_id} with code {referral_code_used}: {e}", exc_info=True)
        return jsonify({"error": "Server error during referral registration."}), 500
    finally:
        db.close()

# --- FULLY UPDATED: initiate_stars_deposit_api (with error handling and lower limit) ---
# --- In app.py ---

# ... (keep all code before this route) ...

# --- Find and REPLACE the initiate_stars_deposit_api function ---
@app.route('/api/initiate_stars_deposit', methods=['POST'])
def initiate_stars_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401

    uid = auth["id"]
    data = flask_request.get_json()
    amount_stars = data.get('amount')

    try:
        amount_stars_int = int(amount_stars)
        
        # --- START OF THE CHANGE ---
        # Enforce the new minimum of 50 Stars. The maximum is set by Telegram.
        if not (25 <= amount_stars_int <= 10000):
             return jsonify({"error": "Amount must be between 50 and 10,000 Stars."}), 400
        # --- END OF THE CHANGE ---
             
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid amount for Stars."}), 400

    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user: return jsonify({"error": "User not found."}), 404

        title = f"Top up {amount_stars_int} Stars"
        description = f"Add {amount_stars_int} Stars to your Case Hunter balance."
        payload = f"stars-topup-{uid}-{uuid.uuid4()}"
        prices = [types.LabeledPrice(label=f"{amount_stars_int} Stars", amount=amount_stars_int)]

        invoice_link = bot.create_invoice_link(
            title=title,
            description=description,
            payload=payload,
            provider_token="", # MUST provide a token, even if empty for XTR. Use your real one.
            currency="XTR",
            prices=prices,
        )

        return jsonify({"status": "success", "invoice_link": invoice_link})

    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            logger.warning(f"Failed to create invoice for user {uid}, likely because they haven't started the bot. Error: {e}")
            return jsonify({
                "status": "error",
                "message": "Could not create payment link. Please start a chat with our bot first and try again."
            }), 400
        
        logger.error(f"Error creating Stars invoice for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "Could not create Stars invoice due to a server error."}), 500
    finally:
        db.close()

# ... (the rest of your code remains unchanged) ...
# NEW API Endpoint to fetch gift listings
@app.route('/api/tonnel_gift_listings/<int:inventory_item_id>', methods=['GET'])
def get_tonnel_gift_listings_api(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data:
        return jsonify({"error": "Authentication failed"}), 401
    
    player_user_id = auth_user_data["id"]
    db = next(get_db())
    tonnel_client = None
    loop = None # Initialize loop to None

    try:
        item_to_withdraw = db.query(InventoryItem).filter(
            InventoryItem.id == inventory_item_id,
            InventoryItem.user_id == player_user_id
        ).first()

        if not item_to_withdraw:
            return jsonify({"error": "Item not found in your inventory."}), 404
        if item_to_withdraw.is_ton_prize:
            return jsonify({"error": "TON prizes cannot be listed for Tonnel withdrawal."}), 400
            
        item_name_for_tonnel = item_to_withdraw.item_name_override or (item_to_withdraw.nft.name if item_to_withdraw.nft else None)
        if not item_name_for_tonnel:
            logger.error(f"Item {inventory_item_id} has no name for Tonnel listing for user {player_user_id}.")
            return jsonify({"error": "Item data is incomplete."}), 500

        if not TONNEL_SENDER_INIT_DATA or not TONNEL_GIFT_SECRET:
            return jsonify({"error": "Withdrawal service configuration error."}), 503

        loop = asyncio.new_event_loop() # Create loop before using client
        asyncio.set_event_loop(loop)

        tonnel_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET)
        
        listings = loop.run_until_complete(
            tonnel_client.fetch_gift_listings(gift_item_name=item_name_for_tonnel, limit=5)
        )
        
        return jsonify(listings)

    except Exception as e:
        logger.error(f"Error fetching Tonnel gift listings for item {inventory_item_id}, user {player_user_id}: {e}", exc_info=True)
        # Attempt to close client session even on error, if client was initialized
        if tonnel_client and loop and not loop.is_closed():
            try:
                loop.run_until_complete(tonnel_client._close_session_if_open())
            except Exception as e_close_on_error:
                logger.error(f"Exception during Tonnel session close on error path: {e_close_on_error}")
        return jsonify({"error": "Server error fetching gift listings."}), 500
    finally:
        # Close client session if initialized and loop is available and not closed
        if tonnel_client and loop and not loop.is_closed():
            try:
                loop.run_until_complete(tonnel_client._close_session_if_open())
            except Exception as e_session_close_final:
                logger.error(f"Exception during final Tonnel session close: {e_session_close_final}")
        
        # Close the loop if it was created
        if loop and not loop.is_closed():
            loop.close()
        
        db.close()

# --- NEW BACKEND ENDPOINT for withdrawing emoji gifts RIGHT---

@app.route('/api/withdraw_emoji_gift', methods=['POST'])
def withdraw_emoji_gift_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401

    uid = auth["id"]
    data = flask_request.get_json()
    inventory_item_id = data.get('inventory_item_id')

    if not inventory_item_id:
        return jsonify({"error": "inventory_item_id is required"}), 400

    db = next(get_db())
    try:
        # Lock the item row to prevent race conditions
        item = db.query(InventoryItem).filter(
            InventoryItem.id == inventory_item_id, 
            InventoryItem.user_id == uid
        ).with_for_update().first()

        if not item:
            return jsonify({"error": "Item not found in your inventory."}), 404
        
        item_name = item.item_name_override or (item.nft.name if item.nft else "Unknown")

        # Check if the item is a valid, withdrawable emoji gift
        if item_name not in EMOJI_GIFTS_BACKEND:
            return jsonify({"error": "This item is not an automatically withdrawable emoji gift."}), 400

        gift_data = EMOJI_GIFTS_BACKEND[item_name]
        
        if bot:
            # Send the gift directly to the user
            bot.send_gift(
                chat_id=uid, 
                gift_id=gift_data['id'],
                text="🎉 Поздравляем с выигрышем!"
            )
            logger.info(f"Successfully sent emoji gift '{item_name}' (ID: {gift_data['id']}) to user {uid}")
            
            # If sending was successful, remove the item from inventory
            db.delete(item)
            db.commit()
            return jsonify({"status": "success", "message": f"Your {item_name} gift has been sent to your chat!"})
        else:
            # This case happens if the bot isn't configured on the server
            return jsonify({"error": "The gift sending service is currently unavailable."}), 503

    except Exception as e:
        db.rollback()
        logger.error(f"Error withdrawing emoji gift for user {uid}, item {inventory_item_id}: {e}", exc_info=True)
        return jsonify({"error": "A server error occurred during withdrawal."}), 500
    finally:
        db.close()

# --- Find and REPLACE the entire open_case_api function ---

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401

    uid = auth["id"]
    data = flask_request.get_json()
    cid = data.get('case_id')
    multiplier = data.get('multiplier', 1)

    if not cid: return jsonify({"error": "case_id required"}), 400
    try:
        multiplier = int(multiplier)
        if multiplier not in [1, 2, 3]: return jsonify({"error": "Invalid multiplier."}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid multiplier format."}), 400

    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404

        target_case = next((c for c in cases_data_backend if c['id'] == cid), None)
        if not target_case: return jsonify({"error": "Case not found"}), 404

        cost_per_case_ton = Decimal(str(target_case['priceTON']))
        total_cost_ton = cost_per_case_ton * Decimal(multiplier)
        user_balance_ton = Decimal(str(user.ton_balance))

        if user_balance_ton < total_cost_ton:
            return jsonify({"error": f"Not enough balance."}), 400

        user.ton_balance = float(user_balance_ton - total_cost_ton)

        prizes_in_case = target_case['prizes']
        
        # --- START OF THE NEW, MORE POWERFUL LUCK BOOST LOGIC ---
        luck_boost_multiplier = BOOSTED_LUCK_USERS.get(uid)
        
        if luck_boost_multiplier:
            logger.info(f"Applying new x{luck_boost_multiplier} reallocation luck boost for user {uid}.")
            
            dynamic_prizes = [p.copy() for p in prizes_in_case]
            valuable_threshold = cost_per_case_ton * VALUABLE_PRIZE_THRESHOLD_MULTIPLIER

            valuable_prizes = []
            common_prizes = []
            total_common_prob = Decimal('0')

            for prize in dynamic_prizes:
                if Decimal(str(prize.get('floor_price', 0))) >= valuable_threshold:
                    valuable_prizes.append(prize)
                else:
                    common_prizes.append(prize)
                    total_common_prob += Decimal(str(prize['probability']))
            
            if valuable_prizes and total_common_prob > 0:
                # 1. Determine how much probability to steal from the common items
                prob_to_reallocate = total_common_prob * BOOSTED_LUCK_REALLOCATION_FACTOR
                
                # 2. Reduce the chance of all common items proportionally
                reduction_factor = (total_common_prob - prob_to_reallocate) / total_common_prob
                for prize in common_prizes:
                    prize['probability'] = float(Decimal(str(prize['probability'])) * reduction_factor)
                
                # 3. Distribute the stolen probability among the valuable items.
                # We'll give it to them based on their original chances (rarer valuable items get a smaller piece of the pie).
                total_original_valuable_prob = sum(Decimal(str(p['probability'])) for p in valuable_prizes)
                
                if total_original_valuable_prob > 0:
                    for prize in valuable_prizes:
                        original_prob = Decimal(str(prize['probability']))
                        share_of_reallocation = original_prob / total_original_valuable_prob
                        bonus_prob = prob_to_reallocate * share_of_reallocation
                        prize['probability'] = float(original_prob + bonus_prob)

                # 4. Re-assemble and re-normalize the final prize list to be perfectly 1.0
                prizes_to_use_for_spin = valuable_prizes + common_prizes
                final_total_prob = sum(p['probability'] for p in prizes_to_use_for_spin)
                if final_total_prob > 0:
                    for prize in prizes_to_use_for_spin:
                        prize['probability'] /= final_total_prob
            else:
                # Fallback to the original list if there are no valuable prizes to boost
                prizes_to_use_for_spin = prizes_in_case
        else:
            prizes_to_use_for_spin = prizes_in_case
        # --- END OF THE LUCK BOOST LOGIC ---

        won_prizes_response_list = []
        total_value_this_spin_ton = Decimal('0')

        for _ in range(multiplier):
            roll = random.random()
            cumulative_probability = 0.0
            chosen_prize_info = None
            
            for p_info in prizes_to_use_for_spin:
                cumulative_probability += p_info['probability']
                if roll <= cumulative_probability:
                    chosen_prize_info = p_info
                    break
            
            if not chosen_prize_info: chosen_prize_info = prizes_to_use_for_spin[-1]

            prize_name = chosen_prize_info['name']
            prize_value_ton = Decimal(str(chosen_prize_info.get('floor_price', 0)))
            total_value_this_spin_ton += prize_value_ton
            db_nft = db.query(NFT).filter(NFT.name == prize_name).first()
            
            is_emoji = prize_name in EMOJI_GIFT_IMAGES
            image_url = EMOJI_GIFT_IMAGES.get(prize_name) if is_emoji else (db_nft.image_filename if db_nft else generate_image_filename_from_name(prize_name))

            inventory_item = InventoryItem(
                user_id=uid, nft_id=db_nft.id if db_nft else None, item_name_override=prize_name,
                item_image_override=image_url, current_value=float(prize_value_ton), is_ton_prize=False
            )
            db.add(inventory_item)
            db.flush()

            won_prizes_response_list.append({
                "id": inventory_item.id, "name": prize_name, "imageFilename": inventory_item.item_image_override,
                "currentValue": inventory_item.current_value, "is_emoji_gift": is_emoji
            })

        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin_ton)
        db.commit()

        return jsonify({
            "status": "success", "won_prizes": won_prizes_response_list, "new_balance_ton": user.ton_balance
        })

    except Exception as e:
        db.rollback()
        logger.error(f"Critical error in open_case for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "A server error occurred."}), 500
    finally:
        db.close()
        
@app.route('/api/spin_slot', methods=['POST'])
def spin_slot_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    data = flask_request.get_json()
    slot_id = data.get('slot_id')

    if not slot_id:
        return jsonify({"error": "slot_id required"}), 400
    
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        target_slot = next((s for s in slots_data_backend if s['id'] == slot_id), None)
        if not target_slot:
            return jsonify({"error": "Slot not found"}), 404
        
        cost = Decimal(str(target_slot['priceTON']))
        if Decimal(str(user.ton_balance)) < cost:
            return jsonify({"error": f"Not enough TON. Need {cost:.2f}"}), 400
        
        user.ton_balance = float(Decimal(str(user.ton_balance)) - cost)
        
        num_reels = target_slot.get('reels_config', 3)
        slot_pool = target_slot['prize_pool']

        if not slot_pool:
            return jsonify({"error": "Slot prize pool is empty or not configured."}), 500
        
        reel_results_data = []
        for _ in range(num_reels):
            rv = random.random()
            cprob = 0
            landed_symbol_spec = None
            for p_info_slot in slot_pool:
                cprob += p_info_slot.get('probability', 0)
                if rv <= cprob:
                    landed_symbol_spec = p_info_slot
                    break
            
            if not landed_symbol_spec:
                landed_symbol_spec = random.choice(slot_pool) if slot_pool else {"name":"Error Symbol","imageFilename":"placeholder.png","is_ton_prize":False,"currentValue":0,"floorPrice":0,"value":0}
            
            reel_results_data.append({
                "name": landed_symbol_spec['name'],
                "imageFilename": landed_symbol_spec.get('imageFilename', generate_image_filename_from_name(landed_symbol_spec['name'])),
                "is_ton_prize": landed_symbol_spec.get('is_ton_prize', False),
                "currentValue": landed_symbol_spec.get('value', landed_symbol_spec.get('floorPrice', 0))
            })
            
        won_prizes_from_slot = []
        total_value_this_spin = Decimal('0')
        
        for landed_item_data in reel_results_data:
            if landed_item_data.get('is_ton_prize'):
                ton_val = Decimal(str(landed_item_data['currentValue']))
                user.ton_balance = float(Decimal(str(user.ton_balance)) + ton_val)
                total_value_this_spin += ton_val

                won_prizes_from_slot.append({
                    "id": f"ton_prize_{int(time.time()*1e6)}_{random.randint(0,99999)}",
                    "name": landed_item_data['name'],
                    "imageFilename": landed_item_data.get('imageFilename', TON_PRIZE_IMAGE_DEFAULT),
                    "currentValue": float(ton_val),
                    "is_ton_prize": True
                })
        
        if num_reels == 3 and len(reel_results_data) == 3:
            first_symbol = reel_results_data[0]
            if not first_symbol.get('is_ton_prize') and \
               first_symbol['name'] == reel_results_data[1]['name'] and \
               first_symbol['name'] == reel_results_data[2]['name']:
                
                won_item_name = first_symbol['name']
                db_nft = db.query(NFT).filter(NFT.name == won_item_name).first()
                
                if db_nft:
                    actual_val = Decimal(str(db_nft.floor_price))
                    inv_item = InventoryItem(
                        user_id=uid,
                        nft_id=db_nft.id,
                        item_name_override=db_nft.name,
                        item_image_override=db_nft.image_filename,
                        current_value=float(actual_val.quantize(Decimal('0.01'))),
                        variant=None,
                        is_ton_prize=False
                    )
                    db.add(inv_item)
                    db.flush()
                    
                    won_prizes_from_slot.append({
                        "id": inv_item.id,
                        "name": inv_item.item_name_override,
                        "imageFilename": inv_item.item_image_override,
                        "floorPrice": float(db_nft.floor_price),
                        "currentValue": inv_item.current_value,
                        "is_ton_prize": False,
                        "variant": inv_item.variant
                    })
                    total_value_this_spin += actual_val
                else:
                    logger.error(f"Slot win: NFT '{won_item_name}' not found in DB! Cannot add to inventory.")

        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        
        db.commit()
        return jsonify({
            "status":"success",
            "reel_results":reel_results_data,
            "won_prizes":won_prizes_from_slot,
            "new_balance_ton":user.ton_balance
        })
    except Exception as e:
        db.rollback()
        logger.error(f"Error in spin_slot for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue during slot spin."}), 500
    finally:
        db.close()


@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    data = flask_request.get_json()
    iid = data.get('inventory_item_id')
    mult_str = data.get('multiplier_str')

    if not all([iid, mult_str]):
        return jsonify({"error": "Missing inventory_item_id or multiplier_str parameter."}), 400
    
    try:
        mult = Decimal(mult_str)
        iid_int = int(iid)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid data format for multiplier or item ID."}), 400
    
    chances = {
        Decimal("1.5"):50,
        Decimal("2.0"):35,
        Decimal("3.0"):25,
        Decimal("5.0"):15,
        Decimal("10.0"):8,
        Decimal("20.0"):3
    }
    if mult not in chances:
        return jsonify({"error": "Invalid multiplier value provided."}), 400
    
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_int, InventoryItem.user_id == uid).with_for_update().first()
        if not item or item.is_ton_prize:
            return jsonify({"error": "Item not found in your inventory or cannot be upgraded."}), 404
        
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user:
            return jsonify({"error": "User not found."}), 404

        if random.uniform(0,100) < chances[mult]:
            orig_val = Decimal(str(item.current_value))
            new_val = (orig_val * mult).quantize(Decimal('0.01'), ROUND_HALF_UP)
            
            increase_in_value = new_val - orig_val
            
            item.current_value = float(new_val)
            item.upgrade_multiplier = float(Decimal(str(item.upgrade_multiplier)) * mult)
            
            user.total_won_ton = float(Decimal(str(user.total_won_ton)) + increase_in_value)
            
            db.commit()
            return jsonify({
                "status":"success",
                "message":f"Upgrade successful! Your {item.item_name_override} is now worth {new_val:.2f} TON.",
                "item":{
                    "id":item.id,
                    "currentValue":item.current_value,
                    "name":item.nft.name if item.nft else item.item_name_override,
                    "imageFilename":item.nft.image_filename if item.nft else item.item_image_override,
                    "upgradeMultiplier":item.upgrade_multiplier,
                    "variant":item.variant
                }
            })
        else:
            name_lost = item.nft.name if item.nft else item.item_name_override
            value_lost = Decimal(str(item.current_value))
            
            user.total_won_ton = float(max(Decimal('0'), Decimal(str(user.total_won_ton)) - value_lost))
            
            db.delete(item)
            db.commit()
            return jsonify({"status":"failed","message":f"Upgrade failed! You lost your {name_lost}.", "item_lost":True})
    except Exception as e:
        db.rollback()
        logger.error(f"Error in upgrade_item for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue during upgrade."}), 500
    finally:
        db.close()

# --- In app.py ---

# ... (keep all your code before this function) ...

# --- Find and REPLACE the entire upgrade_item_v2_api function ---

@app.route('/api/upgrade_item_v2', methods=['POST'])
def upgrade_item_v2_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data:
        return jsonify({"error": "Authentication failed"}), 401
    
    player_user_id = auth_user_data["id"]
    data = flask_request.get_json()
    inventory_item_id_str = data.get('inventory_item_id')
    desired_item_name_str = data.get('desired_item_name')

    if not inventory_item_id_str or not desired_item_name_str:
        return jsonify({"error": "Missing inventory_item_id or desired_item_name."}), 400

    try:
        inventory_item_id = int(inventory_item_id_str)
    except ValueError:
        return jsonify({"error": "Invalid inventory_item_id format."}), 400

    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == player_user_id).with_for_update().first()
        if not user:
            return jsonify({"error": "User not found."}), 404

        item_to_upgrade = db.query(InventoryItem).filter(
            InventoryItem.id == inventory_item_id,
            InventoryItem.user_id == player_user_id
        ).with_for_update().first()

        if not item_to_upgrade:
            return jsonify({"error": "Item to upgrade not found in your inventory."}), 404
        if item_to_upgrade.is_ton_prize:
            return jsonify({"error": "TON prizes cannot be upgraded."}), 400

        value_of_item_to_upgrade = Decimal(str(item_to_upgrade.current_value))
        if value_of_item_to_upgrade <= Decimal('0'):
            return jsonify({"error": "Item to upgrade has no value or invalid value."}), 400

        desired_nft_data = db.query(NFT).filter(NFT.name == desired_item_name_str).first()
        if not desired_nft_data:
            return jsonify({"error": f"Desired item '{desired_item_name_str}' not found as an upgradable NFT."}), 404
        
        value_of_desired_item = Decimal(str(desired_nft_data.floor_price))

        if value_of_desired_item <= value_of_item_to_upgrade:
            return jsonify({"error": "Desired item must have a higher value than your current item."}), 400

        calculated_x = value_of_desired_item / value_of_item_to_upgrade
        x_effective = max(Decimal('1.01'), calculated_x)
        
        # This is the "fair" chance that the frontend displays
        chance_decimal_raw = UPGRADE_MAX_CHANCE * (UPGRADE_RISK_FACTOR ** (x_effective - Decimal('1')))
        displayed_chance = min(UPGRADE_MAX_CHANCE, max(Decimal('0'), chance_decimal_raw))
        
        # --- START OF THE HOUSE EDGE LOGIC ---
        # We take the displayed chance and apply our house edge factor to get the REAL chance.
        actual_server_chance = displayed_chance * UPGRADE_HOUSE_EDGE_FACTOR
        # --- END OF THE HOUSE EDGE LOGIC ---
        
        roll = Decimal(str(random.uniform(0, 100)))
        is_success = roll < actual_server_chance # The roll is compared against the REAL chance
        
        name_of_item_being_upgraded = item_to_upgrade.item_name_override or \
                                      (item_to_upgrade.nft.name if item_to_upgrade.nft else "Unknown Item")
        
        # Log both chances for your own records, this is very useful for debugging and balancing.
        logger.info(f"User {player_user_id} attempting upgrade. Displayed Chance: {displayed_chance:.2f}%, Actual Chance: {actual_server_chance:.2f}%, Roll: {roll:.2f}%.")

        if is_success:
            net_value_increase = value_of_desired_item - value_of_item_to_upgrade
            user.total_won_ton = float(Decimal(str(user.total_won_ton)) + net_value_increase)
            db.delete(item_to_upgrade)
            db.flush()

            new_upgraded_item = InventoryItem(
                user_id=user.id, nft_id=desired_nft_data.id, item_name_override=desired_nft_data.name,
                item_image_override=desired_nft_data.image_filename or generate_image_filename_from_name(desired_nft_data.name),
                current_value=float(value_of_desired_item), upgrade_multiplier=1.0, is_ton_prize=False, variant=None
            )
            db.add(new_upgraded_item)
            db.commit()
            db.refresh(new_upgraded_item)

            logger.info(f"Upgrade for user {player_user_id} was SUCCESSFUL.")

            return jsonify({
                "status": "success",
                "message": f"Upgrade successful! Your {name_of_item_being_upgraded} became {desired_nft_data.name}.",
                "item": { "id": new_upgraded_item.id, "name": new_upgraded_item.item_name_override,
                          "imageFilename": new_upgraded_item.item_image_override, "currentValue": new_upgraded_item.current_value,
                          "is_ton_prize": new_upgraded_item.is_ton_prize, "variant": new_upgraded_item.variant }
            })
        else: # Upgrade failed
            user.total_won_ton = float(max(Decimal('0'), Decimal(str(user.total_won_ton)) - value_of_item_to_upgrade))
            db.delete(item_to_upgrade)
            db.commit()

            logger.info(f"Upgrade for user {player_user_id} FAILED.")

            return jsonify({
                "status": "failed", "message": f"Upgrade failed! Your {name_of_item_being_upgraded} was lost.",
                "item_lost": True, "lost_item_name": name_of_item_being_upgraded,
                "lost_item_value": float(value_of_item_to_upgrade)
            })

    except SQLAlchemyError as sqla_e:
        db.rollback()
        logger.error(f"SQLAlchemyError during upgrade_item_v2 for user {player_user_id}: {sqla_e}", exc_info=True)
        return jsonify({"error": "Database operation failed during upgrade."}), 500
    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected error during upgrade_item_v2 for user {player_user_id}: {e}", exc_info=True)
        return jsonify({"error": "An unexpected server error occurred during upgrade."}), 500
    finally:
        db.close()
        
@app.route('/api/convert_to_ton', methods=['POST'])
def convert_to_ton_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    data = flask_request.get_json()
    iid_convert = data.get('inventory_item_id')

    if not iid_convert:
        return jsonify({"error": "inventory_item_id required."}), 400
    try:
        iid_convert_int = int(iid_convert)
    except ValueError:
        return jsonify({"error": "Invalid inventory_item_id format."}), 400
    
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_convert_int, InventoryItem.user_id == uid).first()
        
        if not user:
            return jsonify({"error": "User not found."}), 404
        if not item:
            return jsonify({"error": "Item not found in your inventory."}), 404
        if item.is_ton_prize:
            return jsonify({"error": "Cannot convert a TON prize item (it's already TON)."}), 400
            
        val_to_add = Decimal(str(item.current_value))
        user.ton_balance = float(Decimal(str(user.ton_balance)) + val_to_add)
        
        item_name_converted = item.nft.name if item.nft else item.item_name_override
        
        user.total_won_ton = float(max(Decimal('0'), Decimal(str(user.total_won_ton)) - val_to_add))
        
        db.delete(item)
        db.commit()
        return jsonify({
            "status":"success",
            "message":f"Item '{item_name_converted}' converted to {val_to_add:.2f} TON.",
            "new_balance_ton":user.ton_balance
        })
    except Exception as e:
        db.rollback()
        logger.error(f"Error in convert_to_ton for user {uid}, item {iid_convert_int}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue during conversion."}), 500
    finally:
        db.close()

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        items_to_sell = [item_obj for item_obj in user.inventory if not item_obj.is_ton_prize]
        if not items_to_sell:
            return jsonify({"status":"no_items","message":"No sellable items in your collection to convert."})
            
        total_value_from_sell = sum(Decimal(str(i_sell.current_value)) for i_sell in items_to_sell)
        user.ton_balance = float(Decimal(str(user.ton_balance)) + total_value_from_sell)
        
        num_items_sold = len(items_to_sell)

        user.total_won_ton = float(max(Decimal('0'), Decimal(str(user.total_won_ton)) - total_value_from_sell))
        
        for i_del in items_to_sell:
            db.delete(i_del)
        
        db.commit()
        return jsonify({
            "status":"success",
            "message":f"All {num_items_sold} sellable items converted for a total of {total_value_from_sell:.2f} TON.",
            "new_balance_ton":user.ton_balance
        })
    except Exception as e:
        db.rollback()
        logger.error(f"Error in sell_all_items for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue during bulk conversion."}), 500
    finally:
        db.close()

@app.route('/api/initiate_deposit', methods=['POST'])
def initiate_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    data = flask_request.get_json()
    amt_str = data.get('amount')

    if amt_str is None:
        return jsonify({"error": "Amount required."}), 400
    try:
        orig_amt = float(amt_str)
    except ValueError:
        return jsonify({"error": "Invalid amount format."}), 400
    
    if not (0.1 <= orig_amt <= 10000):
        return jsonify({"error": "Amount must be between 0.1 and 10000 TON."}), 400
    
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user:
            return jsonify({"error": "User not found."}), 404
        
        # Invalidate any other pending deposits for this user to avoid confusion
        db.query(PendingDeposit).filter(
            PendingDeposit.user_id == uid,
            PendingDeposit.status == 'pending'
        ).update({"status": "cancelled"})

        # Generate a new unique comment
        unique_comment = secrets.token_hex(4) # e.g., 'a1b2c3d4'
        while db.query(PendingDeposit).filter(PendingDeposit.expected_comment == unique_comment).first():
            unique_comment = secrets.token_hex(4)

        final_nano_amt = int(orig_amt * 1e9)
        
        pdep = PendingDeposit(
            user_id=uid,
            original_amount_ton=orig_amt,
            final_amount_nano_ton=final_nano_amt,
            expected_comment=unique_comment,
            expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES)
        )
        db.add(pdep)
        db.commit()
        db.refresh(pdep)
        
        amount_to_send_display = f"{orig_amt:.4f}".rstrip('0').rstrip('.')
        
        return jsonify({
            "status":"success",
            "pending_deposit_id":pdep.id,
            "recipient_address":DEPOSIT_RECIPIENT_ADDRESS_RAW,
            "amount_to_send":amount_to_send_display,
            "final_amount_nano_ton":final_nano_amt,
            "comment":unique_comment,
            "expires_at":pdep.expires_at.isoformat()
        })
    except Exception as e:
        db.rollback()
        logger.error(f"Error in initiate_deposit for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue during deposit initiation."}), 500
    finally:
        db.close()

async def check_blockchain_for_deposit(pdep: PendingDeposit, db_sess: SessionLocal):
    """
    Asynchronously checks the blockchain for a matching deposit transaction based on comment and amount.
    """
    prov = None
    try:
        prov = LiteBalancer.from_mainnet_config(trust_level=2)
        await prov.start_up()

        txs = await prov.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=50)
        
        deposit_found = False
        for tx in txs:
            if not tx.in_msg or not tx.in_msg.is_internal:
                continue
            
            tx_time = dt.fromtimestamp(tx.now, tz=timezone.utc)
            if not (pdep.created_at - timedelta(minutes=5) <= tx_time <= pdep.expires_at + timedelta(minutes=5)):
                continue

            tx_comment = ""
            try:
                cmt_slice = tx.in_msg.body.begin_parse()
                if cmt_slice.remaining_bits >= 32 and cmt_slice.load_uint(32) == 0:
                    tx_comment = cmt_slice.load_snake_string()
            except Exception:
                continue
            
            if tx_comment == pdep.expected_comment:
                if tx.in_msg.info.value_coins == pdep.final_amount_nano_ton:
                    deposit_found = True
                    break
                else:
                    logger.warning(f"Deposit {pdep.id} found matching comment '{pdep.expected_comment}' but with incorrect amount. Expected: {pdep.final_amount_nano_ton}, Received: {tx.in_msg.info.value_coins}.")

        if deposit_found:
            # Credit user logic (same as before)
            usr = db_sess.query(User).filter(User.id == pdep.user_id).with_for_update().first()
            if not usr:
                pdep.status = 'failed_user_not_found'
                db_sess.commit()
                return {"status":"error","message":"User for deposit not found."}
            
            usr.ton_balance = float(Decimal(str(usr.ton_balance)) + Decimal(str(pdep.original_amount_ton)))
            
            if usr.referred_by_id:
                referrer = db_sess.query(User).filter(User.id == usr.referred_by_id).with_for_update().first()
                if referrer:
                    referral_bonus = (Decimal(str(pdep.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                    referrer.referral_earnings_pending = float(Decimal(str(referrer.referral_earnings_pending)) + referral_bonus)
            
            pdep.status = 'completed'
            db_sess.commit()
            return {"status":"success","message":"Deposit confirmed and credited!","new_balance_ton":usr.ton_balance}
        else:
            # Pending/expired logic (same as before)
            if pdep.expires_at <= dt.now(timezone.utc) and pdep.status == 'pending':
                pdep.status = 'expired'
                db_sess.commit()
                return {"status":"expired","message":"This deposit request has expired."}
            
            return {"status":"pending","message":"Transaction not found. Please ensure you sent the exact amount with the correct comment."}
    except Exception as e_bc_check:
        logger.error(f"Blockchain check error for deposit {pdep.id}: {e_bc_check}", exc_info=True)
        return {"status":"error","message":"An error occurred during blockchain verification."}
    finally:
        if prov:
            await prov.close_all()

# --- Replace your existing verify_deposit_api function ---

# --- Find and REPLACE the entire verify_deposit_api function ---
@app.route('/api/verify_deposit', methods=['POST'])
def verify_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    data = flask_request.get_json()
    pid = data.get('pending_deposit_id')
    if not pid: return jsonify({"error": "Pending deposit ID required."}), 400

    db = next(get_db())
    try:
        pdep = db.query(PendingDeposit).filter(PendingDeposit.id == pid, PendingDeposit.user_id == uid).first()
        if not pdep: return jsonify({"error": "Pending deposit not found."}), 404
        if pdep.status == 'completed': return jsonify({"status":"success", "message":"Deposit already confirmed."})
        if pdep.status == 'pending' and pdep.expires_at <= dt.now(timezone.utc):
            pdep.status = 'expired'
            db.commit()
            return jsonify({"status":"expired", "message":"This deposit request has expired."})
    finally:
        db.close()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    deposit_found = loop.run_until_complete(check_blockchain_for_deposit_simple(pdep))
    loop.close()

    if deposit_found:
        db_update = SessionLocal()
        try:
            pdep_update = db_update.query(PendingDeposit).filter(PendingDeposit.id == pid).with_for_update().first()
            usr_update = db_update.query(User).filter(User.id == uid).with_for_update().first()
            
            if pdep_update and usr_update and pdep_update.status == 'pending':
                deposited_ton_amount = Decimal(str(pdep_update.original_amount_ton))
                usr_update.ton_balance = float(Decimal(str(usr_update.ton_balance)) + deposited_ton_amount)
                pdep_update.status = 'completed'

                # --- START OF CHANGE ---
                # Log this successful deposit to our new unified table
                new_deposit_log = Deposit(
                    user_id=uid,
                    ton_amount=float(deposited_ton_amount),
                    deposit_type='TON'
                )
                db_update.add(new_deposit_log)
                # --- END OF CHANGE ---

                if usr_update.referred_by_id:
                    referrer = db_update.query(User).filter(User.id == usr_update.referred_by_id).with_for_update().first()
                    if referrer:
                        referral_bonus = (deposited_ton_amount * DEFAULT_REFERRAL_RATE)
                        if referrer.username and referrer.username.lower() in (name.lower() for name in SPECIAL_REFERRAL_RATES.keys()):
                            referral_bonus = (deposited_ton_amount * SPECIAL_REFERRAL_RATES[referrer.username.lower()])
                        referrer.referral_earnings_pending = float(Decimal(str(referrer.referral_earnings_pending)) + referral_bonus)
                
                db_update.commit()
                return jsonify({"status": "success", "message": "Deposit confirmed and credited!", "new_balance_ton": usr_update.ton_balance})
            else:
                db_update.rollback()
                return jsonify({"status": "pending", "message": "Deposit found, but state changed. Please check again."})

        except Exception as e:
            db_update.rollback()
            logger.error(f"Error updating DB after successful deposit check for {pid}: {e}", exc_info=True)
            return jsonify({"status": "error", "message": "Verification successful, but failed to credit account. Please contact support."})
        finally:
            db_update.close()
    else:
        return jsonify({"status": "pending", "message": "Transaction not found. Please ensure you sent the exact amount with the correct comment."})

# --- Add this new helper function to your Python backend ---

async def check_blockchain_for_deposit_simple(pdep: PendingDeposit) -> bool:
    """
    Asynchronously checks the TON blockchain for a specific deposit.
    This function does NOT interact with the database; it only performs network calls.
    It returns True if a matching transaction is found, otherwise False.
    """
    prov = None
    try:
        # Establish connection to the TON blockchain
        prov = LiteBalancer.from_mainnet_config(trust_level=2)
        await prov.start_up()

        # Fetch the last 50 transactions for the recipient address
        # This count is usually sufficient to find a recent transaction
        txs = await prov.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=50)
        
        deposit_found = False
        for tx in txs:
            # We only care about incoming internal messages (standard TON transfers)
            if not tx.in_msg or not tx.in_msg.is_internal:
                continue
            
            # Check if the transaction time is within a reasonable window of the deposit request
            tx_time = dt.fromtimestamp(tx.now, tz=timezone.utc)
            if not (pdep.created_at - timedelta(minutes=5) <= tx_time <= pdep.expires_at + timedelta(minutes=5)):
                continue

            tx_comment = ""
            try:
                # Attempt to parse the comment from the transaction body
                cmt_slice = tx.in_msg.body.begin_parse()
                # Check for the standard text comment prefix (0x00000000)
                if cmt_slice.remaining_bits >= 32 and cmt_slice.load_uint(32) == 0:
                    tx_comment = cmt_slice.load_snake_string()
            except Exception:
                # If parsing fails (e.g., it's a binary comment or no comment), just skip it
                continue
            
            # The core matching logic:
            if tx_comment == pdep.expected_comment and tx.in_msg.info.value_coins == pdep.final_amount_nano_ton:
                logger.info(f"MATCH FOUND for deposit ID {pdep.id}: Comment '{pdep.expected_comment}' and amount {pdep.final_amount_nano_ton} nTON.")
                deposit_found = True
                break # Exit the loop as soon as we find the correct transaction

        return deposit_found

    except Exception as e_bc_check:
        logger.error(f"Blockchain check network error for deposit {pdep.id}: {e_bc_check}", exc_info=True)
        # In case of a network error, we assume the deposit was not found to be safe
        return False
    finally:
        # Ensure the blockchain provider connection is always closed
        if prov:
            await prov.close_all()

@app.route('/api/request_manual_withdrawal', methods=['POST'])
def request_manual_withdrawal_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401

    uid = auth["id"]
    data = flask_request.get_json()
    inventory_item_id = data.get('inventory_item_id')

    if not inventory_item_id:
        return jsonify({"error": "inventory_item_id required"}), 400

    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == uid).first()

        if not item:
            return jsonify({"error": "Item not found in your inventory."}), 404
        if item.is_ton_prize:
            return jsonify({"error": "TON prizes cannot be withdrawn this way."}), 400

        user = db.query(User).filter(User.id == uid).first()
        if not user:
            return jsonify({"error": "User not found."}), 404

        item_name = item.item_name_override or (item.nft.name if item.nft else "Unknown Item")
        model = item.variant if item.variant else ""

        message = f"Send {item_name} {model} to user {user.first_name} (@{user.username} - {user.id})"

        if bot and TARGET_WITHDRAWER_ID:
            try:
                bot.send_message(TARGET_WITHDRAWER_ID, message)
                # After successfully sending the message, remove the item from inventory
                db.delete(item)
                db.commit()
                return jsonify({"status": "success"})
            except Exception as e:
                logger.error(f"Failed to send withdrawal message: {e}")
                return jsonify({"error": "Failed to notify for withdrawal."}), 500
        else:
            return jsonify({"error": "Bot or target user for withdrawal not configured."}), 500

    except Exception as e:
        db.rollback()
        logger.error(f"Error in request_manual_withdrawal for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue during withdrawal request."}), 500
    finally:
        db.close()


@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db())
    try:
        leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
        
        leaderboard_data = []
        for r_idx, u_leader in enumerate(leaders):
            display_name = u_leader.first_name or u_leader.username or f"User_{str(u_leader.id)[:6]}"
            avatar_char = (u_leader.first_name or u_leader.username or "U")[0].upper()
            
            leaderboard_data.append({
                "rank": r_idx + 1,
                "name": display_name,
                "avatarChar": avatar_char,
                "income": int(u_leader.total_won_ton * TON_TO_STARS_RATE_BACKEND),
                "user_id": u_leader.id
            })
        return jsonify(leaderboard_data)
    except Exception as e:
        logger.error(f"Error in get_leaderboard: {e}", exc_info=True)
        return jsonify({"error":"Could not load leaderboard due to a server error."}), 500
    finally:
        db.close()

# --- In app.py ---

# --- In app.py ---

# --- Find and REPLACE the entire withdraw_referral_earnings_api function ---
@app.route('/api/withdraw_referral_earnings', methods=['POST'])
def withdraw_referral_earnings_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user:
            return jsonify({"error": "User not found."}), 404
        
        # --- START OF THE NEW LOGIC ---
        # Define the lifetime deposit requirement
        MIN_LIFETIME_DEPOSIT_STARS = 200
        MIN_LIFETIME_DEPOSIT_TON = Decimal(str(MIN_LIFETIME_DEPOSIT_STARS)) / Decimal(str(TON_TO_STARS_RATE_BACKEND))
        
        # Query for all of the user's past deposits from our unified 'deposits' table
        all_user_deposits = db.query(Deposit).filter(Deposit.user_id == uid).all()
        
        # Sum the total lifetime deposit amount
        total_lifetime_deposited_ton = sum(Decimal(str(d.ton_amount)) for d in all_user_deposits)
        
        # Check if the user meets the lifetime deposit requirement
        if total_lifetime_deposited_ton < MIN_LIFETIME_DEPOSIT_TON:
            return jsonify({
                "status": "error",
                "message": f"Для вывода реферального заработка вам необходимо сделать депозит минимум {MIN_LIFETIME_DEPOSIT_STARS}⭐ за весь период."
            }), 403 # 403 Forbidden is a good status code
        # --- END OF THE NEW LOGIC ---

        if user.referral_earnings_pending > 0:
            withdrawn_amount_ton = Decimal(str(user.referral_earnings_pending))
            withdrawn_stars = int(withdrawn_amount_ton * Decimal(TON_TO_STARS_RATE_BACKEND))
            
            user.ton_balance = float(Decimal(str(user.ton_balance)) + withdrawn_amount_ton)
            user.referral_earnings_pending = 0.0
            
            db.commit()
            return jsonify({
                "status":"success",
                "message":f"{withdrawn_stars} Stars from your referral earnings have been added to your main balance.",
                "new_balance_ton":user.ton_balance,
                "new_referral_earnings_pending":0.0
            })
        else:
            return jsonify({"status":"no_earnings","message":"You have no referral earnings to withdraw."})
            
    except Exception as e:
        db.rollback()
        logger.error(f"Error withdrawing referral earnings for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue during withdrawal."}), 500
    finally:
        db.close()
        
@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    
    uid = auth["id"]
    data = flask_request.get_json()
    code_txt = data.get('promocode_text', "").strip()

    if not code_txt:
        return jsonify({"status":"error","message":"Promocode text cannot be empty."}), 400
    
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user:
            return jsonify({"status":"error","message":"User not found."}), 404
            
        promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).with_for_update().first()
        if not promo:
            return jsonify({"status":"error","message":"Invalid promocode."}), 404
            
        if promo.activations_left != -1 and promo.activations_left <= 0:
            return jsonify({"status":"error","message":"This promocode has no activations left."}), 400
            
        existing_redemption = db.query(UserPromoCodeRedemption).filter(
            UserPromoCodeRedemption.user_id == user.id,
            UserPromoCodeRedemption.promo_code_id == promo.id
        ).first()
        if existing_redemption:
            return jsonify({"status":"error","message":"You have already redeemed this promocode."}), 400
            
        if promo.activations_left != -1:
            promo.activations_left -= 1
        
        user.ton_balance = float(Decimal(str(user.ton_balance)) + Decimal(str(promo.ton_amount)))
        
        new_redemption = UserPromoCodeRedemption(user_id=user.id, promo_code_id=promo.id)
        db.add(new_redemption)
        db.commit()
        
        return jsonify({
            "status":"success",
            "message":f"Promocode '{code_txt}' redeemed successfully! You received {promo.ton_amount:.2f} TON.",
            "new_balance_ton":user.ton_balance
        })
    except IntegrityError as ie:
        db.rollback()
        logger.error(f"IntegrityError redeeming promocode '{code_txt}' for user {uid}: {ie}", exc_info=True)
        return jsonify({"status":"error","message":"Promocode redemption failed due to a conflict. Please try again."}), 409
    except Exception as e:
        db.rollback()
        logger.error(f"Error redeeming promocode '{code_txt}' for user {uid}: {e}", exc_info=True)
        return jsonify({"status":"error","message":"A server error occurred during promocode redemption."}), 500
    finally:
        db.close()

@app.route('/api/confirm_tonnel_withdrawal/<int:inventory_item_id>', methods=['POST'])
def confirm_tonnel_withdrawal_api(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data:
        return jsonify({"status": "error", "message": "Authentication failed"}), 401
    
    player_user_id = auth_user_data["id"]
    data = flask_request.get_json()
    chosen_gift_details = data.get('chosen_tonnel_gift_details')

    if not chosen_gift_details or not isinstance(chosen_gift_details, dict) or \
       'gift_id' not in chosen_gift_details or 'price' not in chosen_gift_details:
        return jsonify({"status": "error", "message": "Chosen Tonnel gift details are missing or invalid."}), 400

    if not TONNEL_SENDER_INIT_DATA or not TONNEL_GIFT_SECRET:
        logger.error("Tonnel confirm withdrawal: Essential Tonnel ENV VARS not set.")
        return jsonify({"status": "error", "message": "Withdrawal service is currently misconfigured."}), 503
        
    db = next(get_db())
    tonnel_client = None
    loop = None # Initialize loop to None

    try:
        item_to_withdraw = db.query(InventoryItem).filter(
            InventoryItem.id == inventory_item_id,
            InventoryItem.user_id == player_user_id
        ).with_for_update().first()

        if not item_to_withdraw:
            return jsonify({"status": "error", "message": "Item not found in your inventory or already withdrawn."}), 404
        if item_to_withdraw.is_ton_prize:
            return jsonify({"status": "error", "message":"TON prizes cannot be withdrawn this way."}), 400
            
        item_name_withdrawn = item_to_withdraw.item_name_override or (item_to_withdraw.nft.name if item_to_withdraw.nft else "Unknown Item")

        loop = asyncio.new_event_loop() # Create loop before using client
        asyncio.set_event_loop(loop)
        
        tonnel_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET)
        tonnel_result = {}
        
        tonnel_result = loop.run_until_complete(
            tonnel_client.purchase_specific_gift(chosen_gift_details=chosen_gift_details, receiver_telegram_id=player_user_id)
        )

        if tonnel_result and tonnel_result.get("status") == "success":
            value_deducted_from_winnings = Decimal(str(item_to_withdraw.current_value))
            player = db.query(User).filter(User.id == player_user_id).with_for_update().first()
            if player:
                player.total_won_ton = float(max(Decimal('0'), Decimal(str(player.total_won_ton)) - value_deducted_from_winnings))
            
            db.delete(item_to_withdraw)
            db.commit()
            logger.info(f"Item '{item_name_withdrawn}' (Inv ID: {inventory_item_id}, Tonnel Gift ID: {chosen_gift_details['gift_id']}) withdrawn via Tonnel for user {player_user_id}.")
            return jsonify({
                "status": "success",
                "message": f"Your gift '{chosen_gift_details.get('name', item_name_withdrawn)}' has been sent to your Telegram account via Tonnel!",
                "details": tonnel_result.get("details")
            })
        else:
            db.rollback()
            logger.error(f"Tonnel confirm withdrawal failed. Item Inv ID: {inventory_item_id}, User: {player_user_id}, Chosen Gift ID: {chosen_gift_details['gift_id']}. Tonnel API Response: {tonnel_result}")
            return jsonify({"status": "error", "message": f"Withdrawal failed: {tonnel_result.get('message', 'Tonnel API communication error')}"}), 500
            
    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected exception during Tonnel confirm withdrawal. Item Inv ID: {inventory_item_id}, User: {player_user_id}: {e}", exc_info=True)
        # Attempt to close client session even on error, if client was initialized
        if tonnel_client and loop and not loop.is_closed():
            try:
                loop.run_until_complete(tonnel_client._close_session_if_open())
            except Exception as e_close_on_error:
                logger.error(f"Exception during Tonnel session close on error path (confirm_withdrawal): {e_close_on_error}")
        return jsonify({"status": "error", "message": "An unexpected server error occurred. Please try again."}), 500
    finally:
        # Close client session if initialized and loop is available and not closed
        if tonnel_client and loop and not loop.is_closed():
            try:
                loop.run_until_complete(tonnel_client._close_session_if_open())
            except Exception as e_session_close_final:
                logger.error(f"Exception during final Tonnel session close (confirm_withdrawal): {e_session_close_final}")

        # Close the loop if it was created
        if loop and not loop.is_closed():
            loop.close()

        db.close()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=True)
