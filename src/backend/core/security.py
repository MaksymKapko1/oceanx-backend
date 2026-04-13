import os
import logging
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class EncryptionManager:
    def __init__(self):
        self.master_key = os.getenv("MASTER_KEY")

        if not self.master_key:
            logger.warning("ENCRYPTION_MASTER_KEY не найден! Генерирую временный для тестов...")
            self.master_key = Fernet.generate_key().decode()

        self.cipher_suite = Fernet(self.master_key.encode())

    def encrypt_key(self, plain_text_key: str) -> str:
        try:
            encrypted_bytes = self.cipher_suite.encrypt(plain_text_key.encode('utf-8'))
            return encrypted_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"Ошибка шифрования: {e}")
            raise ValueError("Не удалось зашифровать ключ")

    def decrpyt_key(self, encrypted_key: str) -> str:
        try:
            decrypted_bytes = self.cipher_suite.decrypt(encrypted_key.encode('utf-8'))
            return decrypted_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"Ошибка расшифровки: {e}")
            raise ValueError("Не удалось расшифровать ключ")

crypto_manager = EncryptionManager()