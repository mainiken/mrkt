import time
import logging


class ErrorHandler:
    """Обработчик ошибок для бота."""

    def __init__(self, session_manager, logger=None):
        """
        Инициализирует ErrorHandler с менеджером сессий.

        Args:
            session_manager: Объект, управляющий сессиями и токенами.
            logger: Объект логгера. По умолчанию используется корневой логгер.
        """
        self._session_manager = session_manager
        self._logger = logger if logger else logging.getLogger()

    def handle_error(self, error_message: str, error_code: int = None):
        """
        Обрабатывает различные типы ошибок.

        Args:
            error_message: Сообщение об ошибке из лога.
            error_code: Код ошибки, если применимо (например, HTTP статус).
        """
        self._logger.error(f"Обработка ошибки: {error_message}")

        if error_code == 401:
            self._handle_unauthorized_error()
        else:
            self._handle_generic_error(error_message)

    def _handle_unauthorized_error(self):
        """
        Обрабатывает ошибку 401 (неавторизован).

        Пытается пересоздать токен и переподключить сессию.
        Если не удается, уходит в сон и повторяет попытку.
        """
        self._logger.warning("Обнаружена ошибка 401. Попытка пересоздания токена и переподключения сессии.")
        try:
            self._session_manager.recreate_token()
            self._session_manager.reconnect_session()
            self._logger.info("Токен пересоздан и сессия переподключена успешно.")
        except Exception as e:
            self._logger.error(f"Не удалось пересоздать токен/переподключить сессию: {e}. Уходим в сон.")
            sleep_duration = 360  # Начальная продолжительность сна
            while True:
                self._logger.info(f"Сон на {sleep_duration} секунд перед повторной попыткой.")
                time.sleep(sleep_duration)
                try:
                    self._session_manager.recreate_token()
                    self._session_manager.reconnect_session()
                    self._logger.info("Повторная попытка: Токен пересоздан и сессия переподключена успешно.")
                    break  # Выход из цикла после успешного переподключения
                except Exception as retry_e:
                    self._logger.error(f"Повторная попытка не удалась: {retry_e}.")
                    # Увеличиваем время сна, но не более 3600 секунд
                    sleep_duration = min(sleep_duration * 2, 3600)

    def _handle_generic_error(self, error_message: str):
        """
        Обрабатывает общие ошибки.

        Args:
            error_message: Сообщение об ошибке.
        """
        self._logger.error(f"Общая ошибка: {error_message}. Требуется дальнейший анализ.")


# Пример использования (для демонстрации)
# if __name__ == "__main__":
#     class MockSessionManager:
#         def recreate_token(self):
#             print("Mock: Recreating token...")
#             #raise Exception("Mock token recreation failed")
#             pass

#         def reconnect_session(self):
#             print("Mock: Reconnecting session...")
#             #raise Exception("Mock session reconnection failed")
#             pass

#     mock_manager = MockSessionManager()
#     error_handler = ErrorHandler(mock_manager)

#     # Пример обработки ошибки 401
#     #error_handler.handle_error("Не удалось получить информацию о пользователе: 401", 401)

#     # Пример обработки другой ошибки
#     #error_handler.handle_error("Ошибка во время цикла бота: Connection reset by peer") 