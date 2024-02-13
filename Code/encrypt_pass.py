from cryptography.fernet import Fernet
import configparser

# Generate a secret key
key = Fernet.generate_key()
print(key)
cipher_suite = Fernet(key)

# Encrypt the password
def encrypt_password(password):
    encrypted_password = cipher_suite.encrypt(password.encode())
    return encrypted_password.decode()

# Example passwords
db_password = "96Wangchanyan."

# Encrypt the passwords
encrypted_db_password = encrypt_password(db_password)

# Store the encrypted passwords in config.ini
config = configparser.ConfigParser()
config['database'] = {
    'host': 'sales.postgres.database.azure.com',
    'database': 'Dev',
    'user': 'chywang',
    'password': encrypted_db_password
}
config['Paths'] = {
    'csv_file_path' : 'C:/Users/david/allianz_demo/mock data/part1_mock_data.csv'
}

with open('config.ini', 'w') as configfile:
    config.write(configfile)
