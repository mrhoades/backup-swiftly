import time
import logging
from utils import file_crypto
import Crypto

class crypto:

    def __init__(self):
        self.logger = logging.getLogger("BACKUP_SWIFTLY")

    def encrypt_file(self, password, file_to_encrypt, encrypted_file_name):
        """
        Encrypt input file creating a new encrypted file
        """
        self.logger.info("Encrypting file %s",file_to_encrypt)
        start_time = time.time()
        try:
            file_crypto.encrypt_file(password, file_to_encrypt, encrypted_file_name)
            end_time = time.time()
            self.logger.debug( "Elapsed time for encrypting %s: %0.2f seconds", file_to_encrypt, end_time - start_time)
        except IOError as io_error:
            self.logger.exception("Exception while encrypting the file", io_error)
            raise

    def decrypt_file(self, password, file_to_decrypt, decrypted_file_name):
        """
        Decrypt a file creating a new decrypted file
        """
        self.logger.info("Decrypting file %s",file_to_decrypt,)
        start_time = time.time()
        try:
            file_crypto.decrypt_file(password, file_to_decrypt, decrypted_file_name)
            end_time = time.time()
            self.logger.debug( "Elapsed time for decrypting %s: %0.2f seconds", file_to_decrypt, end_time - start_time)
            return True
        except IOError as io_error:
            self.logger.exception("Exception while decrypting the file", io_error)
            return False

    def encode_password(self, password, file_to_encrypt, encrypted_file_name):
        """
        Encrypt input file creating a new encrypted file
        """
        self.logger.info("Encrypting file %s",file_to_encrypt)
        start_time = time.time()
        try:
            file_crypto.encrypt_file(password, file_to_encrypt, encrypted_file_name)
            end_time = time.time()
            self.logger.debug( "Elapsed time for encrypting %s: %0.2f seconds", file_to_encrypt, end_time - start_time)
        except IOError as io_error:
            self.logger.exception("Exception while encrypting the file", io_error)
            raise

    def decode_password(self, password, file_to_decrypt, decrypted_file_name):
        """
        Decrypt a file creating a new decrypted file
        """
        self.logger.info("Decrypting file %s",file_to_decrypt,)
        start_time = time.time()
        try:
            file_crypto.decrypt_file(password, file_to_decrypt, decrypted_file_name)
            end_time = time.time()
            self.logger.debug( "Elapsed time for decrypting %s: %0.2f seconds", file_to_decrypt, end_time - start_time)
            return True
        except IOError as io_error:
            self.logger.exception("Exception while decrypting the file", io_error)
            return False


    def export_keypair(self, basename, key):
        pubkeyfile   = basename + '.pub'
        prvkeyfile   = basename + '.prv'

        self.export_key(prvkeyfile, key)
        self.export_pubkey(pubkeyfile, key)

    def export_key(self, filename, key):
        try:
            f = open(filename, 'w')
        except IOError as e:
            print e
            raise e
        else:
            f.write( key.exportKey() )
            f.close()

    def export_pubkey(self, filename, key):
        try:
            f = open(filename, 'w')
        except IOError as e:
            print e
            raise e
        else:
            f.write( key.publickey().exportKey() )
            f.close()

    def load_key(self, filename):
        try:
            f = open(filename)
        except IOError as e:
            print e
            raise
        else:
            key = Crypto.PublicKey.RSA.importKey(f.read())
            f.close()
            return key
