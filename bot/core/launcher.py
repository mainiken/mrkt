import glob
import asyncio
import argparse
import os
import subprocess
import signal
from copy import deepcopy
from random import uniform
from colorama import init, Fore, Style
import shutil
from typing import Optional

from bot.utils.universal_telegram_client import UniversalTelegramClient
from bot.utils.web import run_web_and_tunnel, stop_web_and_tunnel
from bot.config import settings
from bot.core.agents import generate_random_user_agent
from bot.utils import logger, config_utils, proxy_utils, CONFIG_PATH, SESSIONS_PATH, PROXIES_PATH
from bot.core.tapper import run_tapper, BaseBot, escape_markdown
from bot.core.registrator import register_sessions
from bot.utils.updater import UpdateManager
from bot.exceptions import InvalidSession

from telethon.errors import (
    AuthKeyUnregisteredError, AuthKeyDuplicatedError, AuthKeyError,
    SessionPasswordNeededError
)

from pyrogram.errors import (
    AuthKeyUnregistered as PyrogramAuthKeyUnregisteredError,
    SessionPasswordNeeded as PyrogramSessionPasswordNeededError,
    SessionRevoked as PyrogramSessionRevoked
)

from bot.core.unscribe import ChannelUnsubscriber

init()
shutdown_event = asyncio.Event()

def signal_handler(signum: int, frame) -> None:
    shutdown_event.set()

START_TEXT = f"""
{Fore.RED}ВНИМАНИЕ: @brucelee23 за Pepe сразу удаляю репу!{Style.RESET_ALL}
{Fore.RED}WARNING: @brucelee23 I'll delete Pepe's rep right away!{Style.RESET_ALL}

{Fore.LIGHTMAGENTA_EX}


 ███▄ ▄███▓ ██▀███   ██ ▄█▀▄▄▄█████▓
▓██▒▀█▀ ██▒▓██ ▒ ██▒ ██▄█▒ ▓  ██▒ ▓▒
▓██    ▓██░▓██ ░▄█ ▒▓███▄░ ▒ ▓██░ ▒░
▒██    ▒██ ▒██▀▀█▄  ▓██ █▄ ░ ▓██▓ ░ 
▒██▒   ░██▒░██▓ ▒██▒▒██▒ █▄  ▒██▒ ░ 
░ ▒░   ░  ░░ ▒▓ ░▒▓░▒ ▒▒ ▓▒  ▒ ░░   
░  ░      ░  ░▒ ░ ▒░░ ░▒ ▒░    ░    
░      ░     ░░   ░ ░ ░░ ░   ░      
       ░      ░     ░  ░            
                                    

{Style.RESET_ALL}
{Fore.CYAN}Select action:{Style.RESET_ALL}

    {Fore.GREEN}1. Launch clicker{Style.RESET_ALL}
    {Fore.GREEN}2. Create session{Style.RESET_ALL}
    {Fore.GREEN}3. Create session via QR{Style.RESET_ALL}
    {Fore.GREEN}4. Upload sessions via web (BETA){Style.RESET_ALL}
    {Fore.GREEN}5. Leaving ALL channels (BETA){Style.RESET_ALL}

{Fore.CYAN}Developed by: @mainiken{Style.RESET_ALL}
{Fore.CYAN}Our Telegram channel: {Fore.BLUE}https://t.me/+x8gutImPtaQyN2Ey{Style.RESET_ALL}
"""

API_ID = settings.API_ID
API_HASH = settings.API_HASH

def prompt_user_action() -> int:
    logger.info(START_TEXT)
    while True:
        action = input("> ").strip()
        if action.isdigit() and action in ("1", "2", "3", "4", "5"):
            return int(action)
        logger.warning("Invalid action. Please enter a number between 1 and 5.")

async def process() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--action", type=int, help="Action to perform")
    parser.add_argument("--update-restart", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if not settings.USE_PROXY:
        logger.info(f"Detected {len(get_sessions(SESSIONS_PATH))} sessions | USE_PROXY=False")
    else:
        logger.info(f"Detected {len(get_sessions(SESSIONS_PATH))} sessions | "
                    f"{len(proxy_utils.get_proxies(PROXIES_PATH))} proxies")

    action = args.action
    if not action and not args.update_restart:
        action = prompt_user_action()

    if action == 1:
        if not API_ID or not API_HASH:
            raise ValueError("API_ID and API_HASH not found in the .env file.")
        await run_tasks()
    elif action == 2:
        await register_sessions()
    elif action == 3:
        session_name = input("Enter the session name for QR code authentication: ")
        print("Initializing QR code authentication...")
        subprocess.run(["python", "-m", "bot.utils.loginQR", "-s", session_name])
        print("QR code authentication was successful!")
    elif action == 4:
        logger.info("Starting web interface for uploading sessions...")
        signal.signal(signal.SIGINT, signal_handler)
        try:
            web_task = asyncio.create_task(run_web_and_tunnel())
            await shutdown_event.wait()
        finally:
            web_task.cancel()
            await stop_web_and_tunnel()
            print("Program terminated.")
    elif action == 5:
        if not API_ID or not API_HASH:
             raise ValueError("API_ID and API_HASH not found in the .env file.")
        logger.info("Анализ каналов для отписки...")
        tg_clients = await get_tg_clients()
        if not tg_clients:
            logger.warning("Нет активных сессий для выполнения действия.")
            return

        # Получаем списки каналов для каждого клиента
        client_channel_tasks = []
        unsubscribers = {}
        for client in tg_clients:
            unsubscriber = ChannelUnsubscriber(client)
            unsubscribers[client.session_name] = unsubscriber # Сохраняем экземпляр для последующего использования
            client_channel_tasks.append(asyncio.create_task(unsubscriber.get_all_channel_usernames()))

        # Ожидаем получения всех списков каналов
        all_clients_channels = await asyncio.gather(*client_channel_tasks)

        # Собираем каналы по клиентам в словарь для удобства
        # Используем session_name как ключ для соответствия unsubscribers
        channels_by_client = {tg_clients[i].session_name: all_clients_channels[i] for i in range(len(tg_clients))}

        # Считаем общее количество уникальных каналов для подтверждения
        all_channel_usernames_flat = [channel for channels_list in all_clients_channels for channel in channels_list]
        total_unique_channels = len(set(all_channel_usernames_flat)) # Используем set для уникальных

        if total_unique_channels == 0:
            logger.info("Не найдено каналов для отписки по всем сессиям.")
            return

        logger.info(f"Найдено {total_unique_channels} уникальных каналов для отписки по всем сессиям.")

        # Запрос подтверждения пользователя
        while True:
            response = input(f"Вы уверены, что хотите отписаться от всех {total_unique_channels} каналов? (y/n): ").strip().lower()
            if response in ('y', 'yes'):
                break
            elif response in ('n', 'no'):
                logger.info("Отписка отменена пользователем.")
                return
            else:
                logger.warning("Неверный ввод. Пожалуйста, введите 'y' или 'n'.")

        logger.info("Запуск процесса отписки...")

        # Запускаем задачи отписки для каждого клиента с его списком каналов
        unsubscription_tasks = []
        for session_name, channels_list in channels_by_client.items():
            if channels_list: # Запускаем отписку только если у клиента есть каналы
                # Получаем соответствующий экземпляр Unsubscriber по session_name
                unsubscriber = unsubscribers.get(session_name)
                if unsubscriber:
                    unsubscription_tasks.append(asyncio.create_task(unsubscriber.unsubscribe_from_channels(channels_list)))

        # Ожидаем завершения всех задач отписки
        unsubscribed_counts = await asyncio.gather(*unsubscription_tasks)
        total_unsubscribed_count = sum(unsubscribed_counts)

        logger.info(f"\nПроцесс отписки завершен. Успешно отписались от {total_unsubscribed_count} каналов.")

async def move_invalid_session_to_error_folder(session_name: str) -> None:
    error_dir = os.path.join(SESSIONS_PATH, "error")
    os.makedirs(error_dir, exist_ok=True)
    
    session_patterns = [
        f"{SESSIONS_PATH}/{session_name}.session",
        f"{SESSIONS_PATH}/telethon/{session_name}.session",
        f"{SESSIONS_PATH}/pyrogram/{session_name}.session"
    ]
    
    found = False
    for pattern in session_patterns:
        matching_files = glob.glob(pattern)
        for session_file in matching_files:
            found = True
            if os.path.exists(session_file):
                relative_path = os.path.relpath(os.path.dirname(session_file), SESSIONS_PATH)
                if relative_path == ".":
                    target_dir = error_dir
                else:
                    target_dir = os.path.join(error_dir, relative_path)
                    os.makedirs(target_dir, exist_ok=True)
                
                target_path = os.path.join(target_dir, os.path.basename(session_file))
                try:
                    shutil.move(session_file, target_path)
                    logger.warning(f"Session {session_name} moved to {target_path} due to invalidity")
                except Exception as e:
                    logger.error(f"Error moving session {session_name}: {e}")
    
    if not found:
        logger.error(f"Session {session_name} not found when attempting to move to error folder")

def get_sessions(sessions_folder: str) -> list[str]:
    session_names = glob.glob(f"{sessions_folder}/*.session")
    session_names += glob.glob(f"{sessions_folder}/telethon/*.session")
    session_names += glob.glob(f"{sessions_folder}/pyrogram/*.session")
    return [file.replace('.session', '') for file in sorted(session_names)]

async def get_tg_clients() -> list[UniversalTelegramClient]:
    session_paths = get_sessions(SESSIONS_PATH)

    if not session_paths:
        raise FileNotFoundError("Session files not found")
    tg_clients = []
    for session in session_paths:
        session_name = os.path.basename(session)

        if session_name in settings.blacklisted_sessions:
            logger.warning(f"{session_name} | Session is blacklisted | Skipping")
            continue

        accounts_config = config_utils.read_config_file(CONFIG_PATH)
        session_config: dict = deepcopy(accounts_config.get(session_name, {}))
        if 'api' not in session_config:
            session_config['api'] = {}
        api_config = session_config.get('api', {})
        api = None
        if api_config.get('api_id') in [4, 6, 2040, 10840, 21724]:
            api = config_utils.get_api(api_config)

        if api:
            client_params = {
                "session": session,
                "api": api
            }
        else:
            client_params = {
                "api_id": api_config.get("api_id", API_ID),
                "api_hash": api_config.get("api_hash", API_HASH),
                "session": session,
                "lang_code": api_config.get("lang_code", "en"),
                "system_lang_code": api_config.get("system_lang_code", "en-US")
            }

            for key in ("device_model", "system_version", "app_version"):
                if api_config.get(key):
                    client_params[key] = api_config[key]

        session_config['user_agent'] = session_config.get('user_agent', generate_random_user_agent())
        api_config.update(api_id=client_params.get('api_id') or client_params.get('api').api_id,
                          api_hash=client_params.get('api_hash') or client_params.get('api').api_hash)

        session_proxy = session_config.get('proxy')
        if not session_proxy and 'proxy' in session_config.keys():
            try:
                tg_clients.append(UniversalTelegramClient(**client_params))
                if accounts_config.get(session_name) != session_config:
                    await config_utils.update_session_config_in_file(session_name, session_config, CONFIG_PATH)
            except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, AuthKeyError,
                   SessionPasswordNeededError, PyrogramAuthKeyUnregisteredError,
                    PyrogramSessionPasswordNeededError,
                   PyrogramSessionRevoked, InvalidSession) as e:
                logger.error(f"{session_name} | Session initialization error: {e}")
                await move_invalid_session_to_error_folder(session_name)
            continue

        else:
            if settings.DISABLE_PROXY_REPLACE:
                proxy = session_proxy or next(iter(proxy_utils.get_unused_proxies(accounts_config, PROXIES_PATH)), None)
            else:
                proxy = await proxy_utils.get_working_proxy(accounts_config, session_proxy) \
                    if session_proxy or settings.USE_PROXY else None

            if not proxy and (settings.USE_PROXY or session_proxy):
                logger.warning(f"{session_name} | Didn't find a working unused proxy for session | Skipping")
                continue
            else:
                try:
                    tg_clients.append(UniversalTelegramClient(**client_params))
                    session_config['proxy'] = proxy
                    if accounts_config.get(session_name) != session_config:
                        await config_utils.update_session_config_in_file(session_name, session_config, CONFIG_PATH)
                except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, AuthKeyError,
                      SessionPasswordNeededError, PyrogramAuthKeyUnregisteredError,
                       PyrogramSessionPasswordNeededError,
                      PyrogramSessionRevoked, InvalidSession) as e:
                    logger.error(f"{session_name} | Session initialization error: {e}")
                    await move_invalid_session_to_error_folder(session_name)

    return tg_clients

async def init_config_file() -> None:
    session_paths = get_sessions(SESSIONS_PATH)

    if not session_paths:
        raise FileNotFoundError("Session files not found")
    for session in session_paths:
        session_name = os.path.basename(session)
        parsed_json = config_utils.import_session_json(session)
        if parsed_json:
            accounts_config = config_utils.read_config_file(CONFIG_PATH)
            session_config: dict = deepcopy(accounts_config.get(session_name, {}))
            session_config['user_agent'] = session_config.get('user_agent', generate_random_user_agent())
            session_config['api'] = parsed_json
            if accounts_config.get(session_name) != session_config:
                await config_utils.update_session_config_in_file(session_name, session_config, CONFIG_PATH)

async def run_tasks() -> None:
    await config_utils.restructure_config(CONFIG_PATH)
    await init_config_file()
    
    base_tasks = []
    
    if settings.AUTO_UPDATE:
        update_manager = UpdateManager()
        base_tasks.append(asyncio.create_task(update_manager.run()))
    
    tg_clients = await get_tg_clients()
    # Отправка уведомления о запуске приложения
    if hasattr(settings, 'NOTIFICATION_CHAT_ID') and settings.NOTIFICATION_CHAT_ID:
        bot = BaseBot(None)
        message = escape_markdown(f"✅ Приложение успешно запущено. Активных сессий: {len(tg_clients)}")
        try:
            asyncio.create_task(bot._send_telegram_message(settings.NOTIFICATION_CHAT_ID, message))
        except Exception:
            pass
    client_tasks = [asyncio.create_task(handle_tapper_session(tg_client=tg_client)) for tg_client in tg_clients]
    
    try:
        if client_tasks:
            await asyncio.gather(*client_tasks, return_exceptions=True)
        
        for task in base_tasks:
            if not task.done():
                task.cancel()
                
        await asyncio.gather(*base_tasks, return_exceptions=True)
        
    except asyncio.CancelledError:
        for task in client_tasks + base_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*client_tasks + base_tasks, return_exceptions=True)
        raise
        
async def handle_tapper_session(tg_client: UniversalTelegramClient, stats_bot: Optional[object] = None):
    session_name = tg_client.session_name
    try:
        logger.info(f"{session_name} | Starting session")
        await run_tapper(tg_client=tg_client)
    except InvalidSession as e:
        logger.error(f"Invalid session: {session_name}: {e}")
        await move_invalid_session_to_error_folder(session_name)
    except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, AuthKeyError, 
            SessionPasswordNeededError) as e:
        logger.error(f"Authentication error for Telethon session {session_name}: {e}")
        await move_invalid_session_to_error_folder(session_name)
    except (PyrogramAuthKeyUnregisteredError,
            PyrogramSessionPasswordNeededError, PyrogramSessionRevoked) as e:
        logger.error(f"Authentication error for Pyrogram session {session_name}: {e}")
        await move_invalid_session_to_error_folder(session_name)
    except Exception as e:
        logger.error(f"Unexpected error in session {session_name}: {e}")
    finally:
        logger.info(f"{session_name} | Session ended")
