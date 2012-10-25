#!/usr/bin/env python
# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 HP Software, LLC.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
import sys
import random
import struct
from Crypto.Cipher import AES
import hashlib
from argparse import ArgumentParser
import logging

random.seed()

logger = logging.getLogger("BACKUP_SWIFTLY")

def packing_format():
    return "{0}Q".format("<" if sys.byteorder == "little" else ">")

def encrypt_file(password, input_file_name, output_file_name, block_size=64*1024):
    """ Encrypts a file using AES (CBC mode) with the
        given password. Returns the number of chunksized blocks
        written to the output file.

        block_size:
            Sets the size of the chunk which the function
            uses to read and encrypt the file. Larger chunk
            sizes can be faster for some files and machines.
            block_size must be divisible by 16.
    """
    logger.debug("Encrypt file: %s, %s" % (input_file_name, output_file_name))
    file_size = os.path.getsize(input_file_name)
    initialization_vector = ''.join(chr(random.randint(0, 0xFF)) for i in range(16))
    key = hashlib.sha256(password).digest()
    encoder = AES.new(key, AES.MODE_CBC, initialization_vector)
    block_count = 0
    with open(input_file_name, 'rb') as infile:
        with open(output_file_name, 'wb') as outfile:
            outfile.write(struct.pack(packing_format(), file_size))
            outfile.write(initialization_vector)
            while True:
                block = infile.read(block_size)
                if not len(block):
                    break
                elif len(block) % 16 != 0:
                    block += ' ' * (16 - len(block) % 16)
                outfile.write(encoder.encrypt(block))
                block_count += 1
    return block_count

def decrypt_file(password, input_file_name, output_file_name, block_size=64*1024):
    """ Decrypts a file using AES (CBC mode) with the
        given password.

        block_size:
            Sets the size of the chunk which the function
            uses to read and encrypt the file. Larger chunk
            sizes can be faster for some files and machines.
            block_size must be divisible by 16.
    """
    logger.debug("Decrypt file: %s, %s" % (input_file_name, output_file_name))
    with open(input_file_name, 'rb') as infile:
        file_size_packed = infile.read(struct.calcsize(packing_format()[1]))
        unpadded_size = struct.unpack(packing_format(), file_size_packed)[0]
        initialization_vector = infile.read(16)
        key = hashlib.sha256(password).digest()
        decoder = AES.new(key, AES.MODE_CBC, initialization_vector)
        block_count = 0
        with open(output_file_name, 'wb') as outfile:
            while True:
                block = infile.read(block_size)
                if not len(block):
                    break
                outfile.write(decoder.decrypt(block))
                block_count += 1
            outfile.truncate(unpadded_size)
    return block_count

def main(args):
    parser = ArgumentParser(description="Encrypts/Decrypts a file using AES-CBC")
    parser.add_argument('verb', choices=['encrypt', 'decrypt'])
    parser.add_argument('infile')
    parser.add_argument('outfile')
    parser.add_argument('password')

    args = parser.parse_args(args)

    func = encrypt_file if args.verb == 'encrypt' else decrypt_file
    func(args.password, args.infile, args.outfile)

def main_noargs():
    main(sys.argv[1:])

if __name__ == '__main__':
    main_noargs()

