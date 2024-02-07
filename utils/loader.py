import os

from cryptography.fernet import Fernet


class Loader():
  def __init__(self):
    self.f = Fernet(os.getenv('FERNET_KEY'))


  def encrypt_file(self, file_path: str):
    with open(file_path, 'rb') as file:
      file_data = file.read()

    encrypted_data = self.f.encrypt(file_data)

    with open(file_path, 'wb') as file:
      file.write(encrypted_data)

  def decrypt_file(self, file_path: str):
    with open(file_path, 'rb') as file:
      file_data = file.read()

    decrypted_data = self.f.decrypt(file_data)

    with open(file_path, 'wb') as file:
      file.write(decrypted_data)

