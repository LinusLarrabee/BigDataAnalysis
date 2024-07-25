import bcrypt

password = 'tK2uhlsp'
hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
print(hashed.decode('utf-8'))

admin