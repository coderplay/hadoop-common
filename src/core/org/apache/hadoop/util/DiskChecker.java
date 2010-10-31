/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Class that provides utility functions for checking disk problem
 */

public class DiskChecker {

  public static class DiskErrorException extends IOException {
    public DiskErrorException(String msg) {
      super(msg);
    }
  }
    
  public static class DiskOutOfSpaceException extends IOException {
    public DiskOutOfSpaceException(String msg) {
      super(msg);
    }
  }
      
  /** 
   * The semantics of mkdirsWithExistsCheck method is different from the mkdirs
   * method provided in the Sun's java.io.File class in the following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly 
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   * <p>
   * mkdirsWithExistsCheck与Sun的java.io.File类所提供的mkdirs方法，它们的语意在
   * 以下方面有所不同: 
   * 建立不存在的父目录 ，此方法不管mkdir在何处失败, 都会检查目录是否存在（因为那个
   * 目录有可能已经由其它进程创建了）。如果对于看上去不存在的目录，mkdir()和
   * exists()检查都失败时，我们返回一个错误信号(返回false)。
   * 而当尝试创建一个已存在的目录或者mkdir失败时,Sun的mkdirs就会产生一个error信号。
   * 
   * 说白了就是mkdirsWithExistsCheck当目录已存在时，返回true; Sun的mkdirs则会返回
   * false.
   * 
   * @param dir
   * @return true on success, false on failure
   */
  public static boolean mkdirsWithExistsCheck(File dir) {
    if (dir.mkdir() || dir.exists()) {
      return true;
    }
    File canonDir = null;
    try {
      canonDir = dir.getCanonicalFile();
    } catch (IOException e) {
      return false;
    }
    String parent = canonDir.getParent();
    return (parent != null) && 
           (mkdirsWithExistsCheck(new File(parent)) &&
                                       // 此处为什么还在判断一次??
                                       // 与dir.mkdir() || dir.exists()有啥不同?
                                      (canonDir.mkdir() || canonDir.exists()));
  }
  
  /**
   * Create the directory if it doesn't exist and 
   * 如果不存在，则建立目录。
   * @param dir
   * @throws DiskErrorException
   */
  public static void checkDir(File dir) throws DiskErrorException {    
    // 如果不存在，则建立目录。建立目录失败，就抛出异常
    if (!mkdirsWithExistsCheck(dir))
      throw new DiskErrorException("can not create directory: " 
                                   + dir.toString());
    // ---------------- 建立成功后 ---------------- 
 
    // 如果它不是一个目录(例如，是一个文件)，则抛出异常    
    if (!dir.isDirectory())
      throw new DiskErrorException("not a directory: " 
                                   + dir.toString());
    // 如果不能读，则抛出异常    
    if (!dir.canRead())
      throw new DiskErrorException("directory is not readable: " 
                                   + dir.toString());
    // 如果不能写，则抛出异常            
    if (!dir.canWrite())
      throw new DiskErrorException("directory is not writable: " 
                                   + dir.toString());
  }

  private static void checkPermission(Path dir, 
                                     FsPermission expected, FsPermission actual) 
  throws IOException {
    // Check for permissions
    if (!actual.equals(expected)) {
      throw new IOException("Incorrect permission for " + dir + 
                            ", expected: " + expected + ", while actual: " + 
                            actual);
    }

  }
  
  /** 
   * Create the directory or check permissions if it already exists.
   * 
   * The semantics of mkdirsWithExistsAndPermissionCheck method is different 
   * from the mkdirs method provided in the Sun's java.io.File class in the 
   * following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly 
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   * @param localFS local filesystem
   * @param dir directory to be created or checked
   * @param expected expected permission
   * @return true on success, false on failure
   */
  public static boolean mkdirsWithExistsAndPermissionCheck(
      LocalFileSystem localFS, Path dir, FsPermission expected) 
  throws IOException {
    File directory = new File(dir.makeQualified(localFS).toUri().getPath());
    if (!directory.exists()) {
      boolean created = mkdirsWithExistsCheck(directory);
      if (created) {
        localFS.setPermission(dir, expected);
        return true;
      }
    }

    checkPermission(dir, expected, localFS.getFileStatus(dir).getPermission());
    return true;
  }
  
  /**
   * Create the local directory if necessary, check permissions and also ensure 
   * it can be read from and written into.
   * @param localFS local filesystem
   * @param dir directory
   * @param expected permission
   * @throws DiskErrorException
   * @throws IOException
   */
  public static void checkDir(LocalFileSystem localFS, Path dir, 
                              FsPermission expected) 
  throws DiskErrorException, IOException {
    if (!mkdirsWithExistsAndPermissionCheck(localFS, dir, expected))
      throw new DiskErrorException("can not create directory: " 
                                   + dir.toString());

    FileStatus stat = localFS.getFileStatus(dir);
    FsPermission actual = stat.getPermission();
    
    if (!stat.isDir())
      throw new DiskErrorException("not a directory: " 
                                   + dir.toString());
            
    FsAction user = actual.getUserAction();
    if (!user.implies(FsAction.READ))
      throw new DiskErrorException("directory is not readable: " 
                                   + dir.toString());
            
    if (!user.implies(FsAction.WRITE))
      throw new DiskErrorException("directory is not writable: " 
                                   + dir.toString());
  }

}

