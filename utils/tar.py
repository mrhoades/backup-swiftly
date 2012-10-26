
import os
import tarfile
import logging

class tar:
    def __init__(self):
        self.logger = logging.getLogger("BACKUP_SWIFTLY")

    def create(self, path, tar_name, list_files_to_ignore=None):

        self.assert_path_exists(path)

        try:
            dir_size = 0
            target_name = '%s%s' % (tar_name, '.tar.gz')
            leading_path = os.path.relpath(path, '/') + '/'
            self.logger.info('Tar and compress path \'%s\' to file with name \'%s\'',path,target_name)
            with tarfile.open(target_name, 'w:gz') as tar:
                for (path, dirs, files) in os.walk(path):
                    for file in files:

                        filename = os.path.join(path, file)
                        try:
                            if os.path.exists(filename):
                                dir_size += os.path.getsize(filename)
                        except Exception as file_size_exception:
                            self.logger.exception('exception reading file size: %s',file_size_exception)

                        final_member_name = filename.replace(leading_path, '')

                        #self.logger.debug('Adding file %s', final_member_name)
                        tar.add(name=filename, arcname=final_member_name)

            self.logger.info('Completed tar and compressing files with size of %0.1f MB'%(dir_size/(1024*1024.0)))
            return target_name
        except Exception as tarfile_exception:
            self.logger.exception('tar snapshot failed : %s',tarfile_exception)
            raise Exception("TarCreationFailed",tarfile_exception)


    def extract(self, dest_path, tar_name):
        try:
            with tarfile.open(tar_name, 'r:gz') as tar_file:
                tar_file.extractall(dest_path)
                return True
        except Exception as tarfile_exception:
            self.logger.exception('untar/decompress snapshot failed: %s',tarfile_exception)
            return False


    def get_directory_size(self, directory):
        dir_size = 0
        for (path, dirs, files) in os.walk(directory):
            for file in files:
                filename = os.path.join(path, file)
                dir_size += os.path.getsize(filename)
        return dir_size

    def assert_path_exists(self, filepath):
        if not os.path.exists(filepath):
            raise Exception("FileDoesNotExist", "Path '" + filepath + "' does NOT exist.")
