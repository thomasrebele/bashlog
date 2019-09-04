package common;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadTools {

  public final static Logger log = LoggerFactory.getLogger(DownloadTools.class);

  public static void download(String url, String path) throws IOException {
    log.info("downloading " + url);
    URL website = new URL(url);
    ReadableByteChannel rbc = Channels.newChannel(website.openStream());
    if (path.endsWith("/")) {
      new File(path).mkdirs();
    }
    if (new File(path).isDirectory()) {
      path += website.getFile().replaceAll(".*/", "");
    }
    try (FileOutputStream fos = new FileOutputStream(path)) {
      fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
    }
  }

  // from http://java-tweets.blogspot.fr/2012/07/untar-targz-file-with-apache-commons.html
  static void untar(String archive, String dstPath, Collection<String> list) throws IOException {
    final int BUFFER = 65536;
    // TODO Auto-generated method stub
    FileInputStream fin = new FileInputStream(archive);
    BufferedInputStream in = new BufferedInputStream(fin);
    GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
    TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn);

    TarArchiveEntry entry = null;

    /** Read the tar entries using the getNextEntry method **/
    while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
      if (list != null && !list.contains(entry.getName())) {
        continue;
      }

      /** If the entry is a directory, create the directory. **/
      if (entry.isDirectory()) {
        File f = new File(dstPath + entry.getName());
        f.mkdirs();
      } else {
        int count;
        byte data[] = new byte[BUFFER];
        File f = new File(dstPath + entry.getName());
        f.getParentFile().mkdirs();

        FileOutputStream fos = new FileOutputStream(dstPath + entry.getName());
        BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER);
        while ((count = tarIn.read(data, 0, BUFFER)) != -1) {
          dest.write(data, 0, count);
        }
        dest.close();
      }
    }

    /** Close the input stream **/

    tarIn.close();
  }

  // from https://www.mkyong.com/java/how-to-decompress-files-from-a-zip-file
  public static void unzip(String zipFile, String outputFolder) {
    byte[] buffer = new byte[1024];
    try {
      //create output directory is not exists
      File folder = new File(outputFolder);
      if (!folder.exists()) {
        folder.mkdir();
      }

      //get the zip file content
      ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
      ZipEntry ze = zis.getNextEntry();
      while (ze != null) {
        String fileName = ze.getName();
        File newFile = new File(outputFolder + File.separator + fileName);

        //create all non exists folders
        //else you will hit FileNotFoundException for compressed folder
        new File(newFile.getParent()).mkdirs();

        FileOutputStream fos = new FileOutputStream(newFile);
        int len;
        while ((len = zis.read(buffer)) > 0) {
          fos.write(buffer, 0, len);
        }

        fos.close();
        ze = zis.getNextEntry();
      }

      zis.closeEntry();
      zis.close();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  public static int[] toIntArray(List<Integer> intList) {
    return intList.stream().mapToInt(Integer::intValue).toArray();
  }

  public static int[] array(int length, int initialValue) {
    int[] array = new int[length];
    Arrays.fill(array, initialValue);
    return array;
  }

}
