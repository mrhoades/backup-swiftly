from httplib import HTTPException
import os
import time
import socket
import logging
import re
from utils import swiftapi


class swift:

    def __init__(self, hp_cloud_creds):

        self.opts = {'auth' : hp_cloud_creds.auth_url   ,
                    'user' : hp_cloud_creds.tenant_name +":" + hp_cloud_creds.username,
                    'key' : hp_cloud_creds.password,
                    'snet' : False,
                    'prefix' : '',
                    'auth_version' : '2.0'}

        self.logger = logging.getLogger("BACKUP_SWIFTLY")


    def upload(self, container_name, path_to_upload_file):

        self.logger.info('Uploading file to swift container: ' + container_name + ":" + path_to_upload_file)
        file_size = (os.path.getsize(path_to_upload_file)/(1024*1024.0))
        self.logger.info('Size: %0.5f MB'%file_size)

        if(file_size > 5100):
            raise Exception("File is too large to upload to swift. Current script supports a maximum size of 5GB")

        try:
            self.create_swift_container_if_not_exists(container_name)
            start_time = time.time()
            swiftapi.st_upload(self.opts, container_name, path_to_upload_file)
            end_time = time.time()
            self.logger.info('Completed uploading file %s in %0.2f seconds: ',path_to_upload_file,end_time-start_time)

        except (swiftapi.ClientException, OSError, HTTPException, socket.error, Exception), err:

            self.logger.exception('Failed to upload file to swift ' + str(err))


    def download(self, container_name, swift_file_name, save_to_file_name):
        self.logger.info('Downloading file from swift: ' + container_name + ":" + swift_file_name)

        try:
            cont = swiftapi.st_get_container(self.opts, container_name)
            if not len(cont):
                self.logger.error('Target swift container \''+ container_name + '\' does not exist')
                return False
            start_time = time.time()
            swiftapi.st_download(self.opts, container_name, swift_file_name, save_to_file_name)
            end_time = time.time()
            self.logger.info('Completed downloading file %s in %0.2f seconds: ',swift_file_name,end_time-start_time)
            return True

        except (swiftapi.ClientException, HTTPException, socket.error), err:
            self.logger.exception('Failed to download file from swift: %s', err)
            return False


    def list_container_items(self, container_name):
        self.logger.info('List items in container: ' + container_name)
        try:
            container_items = swiftapi.st_list(self.opts, container_name)
            for item in container_items:
                self.logger.info(item['name'])
            return True
        except (swiftapi.ClientException, HTTPException, socket.error), err:
            self.logger.exception('Failed to list files in swift: %s', err)
            return False

    def list_containers(self):
        self.logger.info('List containers')
        try:
            container_items = swiftapi.st_list(self.opts, None)
            for item in container_items:
                self.logger.info(item['name'])
            return True
        except (swiftapi.ClientException, HTTPException, socket.error), err:
            self.logger.exception('Failed to list files in swift: %s', err)
            return False


    def get_swift_item_list_using_pattern(self, container_name, swift_item_name_or_pattern):
        self.logger.info('List items in container: ' + container_name)
        try:
            swift_item_list = []

            container_items = swiftapi.st_list(self.opts, container_name)

            if not container_items:
                raise Exception("No items found in container with name: " + container_name)

            for item in container_items:
                if swift_item_name_or_pattern == None:
                    swift_item_list.append(str(item['name']))
                elif(re.findall(swift_item_name_or_pattern,str(item['name']))):
                    self.logger.info("MATCHED ITEM: " + str(item['name']))
                    swift_item_list.append(str(item['name']))

            return swift_item_list

        except (swiftapi.ClientException, HTTPException, socket.error), err:
            self.logger.exception('Failed find file in swift that matches pattern files in swift: %s', err)
            return False


    def create_swift_container_if_not_exists(self, swift_container_name):

        swiftContainer = swiftapi.st_get_container(self.opts, swift_container_name)

        if not len(swiftContainer):
            self.logger.debug("Create swift container \'%s\' since it does not exist",swift_container_name)
            swiftapi.st_create_container(self.opts, swift_container_name)

    def delete_file_from_swift(self, swift_container_name, filename):
        swiftapi.st_delete(self.opts, swift_container_name, filename)
