import filecmp
import os

class compare:

    def are_dirs_equal(self,dir1, dir2):
        """
         Compare two directories recursively
         Return true if trees are the same, false if they aren't
        """
        dirCompare = filecmp.dircmp(dir1, dir2)

        if len(dirCompare.funny_files) > 0 or len(dirCompare.left_only) > 0 or len(dirCompare.right_only) > 0:
            return False

        (_,mismatch,errors) =  filecmp.cmpfiles(dir1, dir2, dirCompare.common_files, shallow=False)

        if len(mismatch) > 0 or len(errors) > 0:
            return False

        for dir in dirCompare.common_dirs:

            newDir1 = os.path.join(dir1, dir)
            newDir2 = os.path.join(dir2, dir)

            if not self.are_dirs_equal(newDir1, newDir2):
                return False

        return True
