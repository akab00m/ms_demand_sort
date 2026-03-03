"""Получение и сохранение токена доступа МойСклад."""

import base64
import sys
from pathlib import Path
from threading import Thread

# Добавляем корневую директорию проекта в путь импорта
sys.path.insert(0, str(Path(__file__).parent.parent))

import keyring
import requests
from colorama import Fore, Style, init

init(autoreset=True)


def get_credentials() -> tuple[str, str]:
    """Запрос логина и пароля у пользователя."""
    login = input("Введите логин: ")
    password = input("Введите пароль: ")
    return login, password


def encode_credentials(login: str, password: str) -> str:
    """Кодирование учётных данных в Base64."""
    credentials = f"{login}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    return encoded_credentials


def get_access_token(encoded_credentials: str) -> str | None:
    """Получение токена доступа через API МойСклад."""
    url = "https://api.moysklad.ru/api/remap/1.2/security/token"
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Accept-Encoding": "gzip",
    }
    print(f"{Fore.CYAN}Отправка запроса на URL: {url}{Style.RESET_ALL}")
    response = requests.post(url, headers=headers, timeout=30)
    if response.status_code == 201:
        token_data: dict[str, str] = response.json()
        access_token = token_data.get("access_token")
        print(f"{Fore.GREEN}Успешно получен новый токен.{Style.RESET_ALL}")
        return access_token
    print(
        f"{Fore.RED}Ошибка при получении токена: "
        f"{response.status_code} {response.text}{Style.RESET_ALL}"
    )
    return None


def save_token_to_credential_manager(token: str) -> None:
    """Сохранение токена в Windows Credential Manager."""
    keyring.set_password("moysklad", "access_token", token)
    print(f"{Fore.GREEN}Токен успешно сохранен в Credential Manager.{Style.RESET_ALL}")


def main() -> None:
    """Основная функция получения и сохранения токена."""
    token = keyring.get_password("moysklad", "access_token")
    if token:
        print(f"{Fore.GREEN}Токен уже существует: {token}{Style.RESET_ALL}")
    else:
        print(f"{Fore.YELLOW}Токен не найден. Получение нового токена...{Style.RESET_ALL}")
        login, password = get_credentials()
        encoded_credentials = encode_credentials(login, password)
        access_token = get_access_token(encoded_credentials)
        if access_token:
            save_token_to_credential_manager(access_token)
            print(f"{Fore.GREEN}Новый токен сохранен: {access_token}{Style.RESET_ALL}")


if __name__ == "__main__":
    thread = Thread(target=main)
    thread.start()
    thread.join()
