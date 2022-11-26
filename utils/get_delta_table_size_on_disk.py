#works well with partitions
import math

def byteFormat_py(filesize):
    if filesize <=0 : 
        return "0 B"
    units = ["B", "kB", "MB", "GB", "TB", "PB", "EB"]
    grp = (math.log10(filesize) // math.log10(1024))
  
    size = filesize / math.pow(1024, grp)
    bytestring = "{0:3.3f} {1}".format(size, units[int(grp)])
  
    return bytestring

def computeFileStats_py(path):
    bytes, count = (0, 0)
    try:
        files = [file for file in dbutils.fs.ls(path) if "_delta_log" not in file.path]
    except:
        return (0, "0 NA")
  
    while len(files) != 0:
    #print(count)
        fileInfo = files.pop()
        if fileInfo.isDir() == False:
            count += 1
            bytes += fileInfo.size
        else:
            files.extend(dbutils.fs.ls(fileInfo.path))
      
    return (count, byteFormat_py(bytes))

# Call the function with path of the delta table
computeFileStats_py("dbfs:/user/hive/warehouse/cummins.db/ngca")