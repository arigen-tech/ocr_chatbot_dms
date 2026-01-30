"""
AES-128 GCM Encryption/Decryption utilities.
Matches the Java encryption implementation for encrypted files.
"""

import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from io import BytesIO

# Constants matching Java implementation
GCM_IV_LENGTH = 12  # Standard GCM IV length (bytes)
GCM_TAG_LENGTH = 128  # Tag length in bits
ALGO = "AES"
TRANSFORMATION = "AES/GCM/NoPadding"


class AESGCMEncryption:
    """Handles AES-128 GCM encryption and decryption of files."""
    
    def __init__(self, secret_key: bytes):
        """
        Initialize with a 16-byte (128-bit) AES key.
        
        Args:
            secret_key: 16 bytes for AES-128
        
        Raises:
            ValueError: If key is not 16 bytes
        """
        if len(secret_key) != 16:
            raise ValueError(f"AES-128 requires 16-byte key, got {len(secret_key)}")
        self.secret_key = secret_key

    def encrypt_stream(self, input_stream, output_stream) -> None:
        iv = os.urandom(GCM_IV_LENGTH)
        output_stream.write(iv)

        cipher = Cipher(
            algorithms.AES(self.secret_key),
            modes.GCM(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()

        buffer_size = 8192
        while True:
            chunk = input_stream.read(buffer_size)
            if not chunk:
                break
            output_stream.write(encryptor.update(chunk))

        encryptor.finalize()

        # IMPORTANT: write authentication tag (16 bytes)
        output_stream.write(encryptor.tag)


    def decrypt_stream(self, encrypted_input_stream):
        iv = encrypted_input_stream.read(GCM_IV_LENGTH)
        if len(iv) != GCM_IV_LENGTH:
            raise IllegalStateException("Invalid encrypted file (IV missing)")

        encrypted_data = encrypted_input_stream.read()

        # Last 16 bytes are the GCM tag
        ciphertext = encrypted_data[:-16]
        tag = encrypted_data[-16:]

        cipher = Cipher(
            algorithms.AES(self.secret_key),
            modes.GCM(iv, tag),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()

        plaintext = decryptor.update(ciphertext) + decryptor.finalize()
        return BytesIO(plaintext)


    def encrypt_file(self, input_path: str, output_path: str) -> None:
        """
        Encrypt a file and save to output path.
        
        Args:
            input_path: Path to plaintext file
            output_path: Path to save encrypted file
        """
        try:
            with open(input_path, 'rb') as in_file:
                with open(output_path, 'wb') as out_file:
                    self.encrypt_stream(in_file, out_file)
        except Exception as e:
            raise Exception(f"File encryption failed: {str(e)}")

    def decrypt_file(self, input_path: str, output_path: str) -> None:
        """
        Decrypt a file and save to output path.
        
        Args:
            input_path: Path to encrypted file
            output_path: Path to save decrypted file
        """
        try:
            with open(input_path, 'rb') as in_file:
                decrypted = self.decrypt_stream(in_file)
                with open(output_path, 'wb') as out_file:
                    out_file.write(decrypted.getvalue())
        except Exception as e:
            raise Exception(f"File decryption failed: {str(e)}")

    @staticmethod
    def is_encrypted_file(file_path: str) -> bool:
        """
        Heuristic check if file is encrypted (very basic - checks if first bytes look like valid IV).
        Note: This is not foolproof. Consider using file extensions like .enc or metadata.
        
        Args:
            file_path: Path to file to check
        
        Returns:
            True if file might be encrypted, False otherwise
        """
        try:
            with open(file_path, 'rb') as f:
                # For true identification, use file extensions or metadata
                # This is a basic check - encrypted files should be larger due to IV + tag
                file_size = os.path.getsize(file_path)
                return file_size >= GCM_IV_LENGTH + 16  # At least IV + minimum ciphertext + tag
        except Exception:
            return False


class IllegalStateException(Exception):
    """Custom exception to match Java IllegalStateException."""
    pass
