from cryptography.fernet import Fernet
import configparser


def decrypt_password(encrypted_password):
    """
    HIGH CONFIDENTIAL
    to decrypt password
    arg:
        encrypted pass
    
    return:
        decrypted pass
    """
    
    # Read the encrypted passwords from config.ini
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Get the encrypted password
    encrypted_db_password = config['database']['password']
    # encrypted_other_password = config['other']['password']

    # Create a Fernet cipher using the key
    key = b'YYTKjyYxviRnDw7_a_1tmLnnJ0bgzdBHi4HjB6m3Omw='
    cipher_suite = Fernet(key)

    # Decrypt the password
    decrypted_password = cipher_suite.decrypt(encrypted_password.encode())
    return decrypted_password.decode()

