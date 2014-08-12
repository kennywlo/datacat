
package org.srs.vfs;

import java.nio.file.InvalidPathException;

/**
 *
 * @author bvan
 */
public class PathUtils {
    

    public static String normalize(String path){
        String checked = normalizeSeparators(path);
        return normalize(checked, offsets(checked));
    }
    
    public static String getFileName(String path){
        return getFileName(path, offsets(path));
    }
    
    public static String getParentPath(String path){
        return getParentPath(path, offsets(path));
    }
    
    public static String getFileName(String path, int[] offsets){
        int count = offsets.length;

        // no elements so no name
        if (count == 0)
            return null;

        // one name element and no root component
        if (count == 1 && path.length() > 0 && path.charAt(0) != '/')
            return path;

        int lastOffset = offsets[count-1];
        return path.substring(lastOffset, path.length());
    }
    
    public static String subpath(String path, int beginIndex, int endIndex){
        return subpath(path, beginIndex, endIndex, offsets(path));
    }
    
    public static String absoluteSubpath(String path, int endIndex, int[] offsets){
        if (endIndex > offsets.length)
            throw new IllegalArgumentException();
        if (endIndex < 0) {
            throw new IllegalArgumentException();
        }
        // starting offset and length
        int end;
        if (endIndex == offsets.length) {
            end = path.length();
        } else {
            end = offsets[endIndex] - 1;
        }
        return path.substring(0, end);
    }
    
    public static String subpath(String path, int beginIndex, int endIndex, int[] offsets){
        if (beginIndex < 0)
            throw new IllegalArgumentException();
        if (beginIndex >= offsets.length)
            throw new IllegalArgumentException();
        if (endIndex > offsets.length)
            throw new IllegalArgumentException();
        if (beginIndex >= endIndex) {
            throw new IllegalArgumentException();
        }

        // starting offset and length
        int begin = offsets[beginIndex];
        int end;
        if (endIndex == offsets.length) {
            end = path.length();
        } else {
            end = offsets[endIndex] - 1;
        }
        return path.substring(begin, end);
    }
    
    public static String getParentPath(String path, int[] offsets){
        int count = offsets.length;
        if (count == 0) {
            // no elements so no parent
            return null;
        }
        int len = offsets[count-1] - 1;
        if (len <= 0) {
            // parent is root only (may be null)
            return "/";
        }        
        return path.substring(0,len);
    }
    
    public static String resolve(String base, String child){
        int baseLength = base.length();
        int childLength = child.length();
        if (childLength == 0)
            return base;
        if (baseLength == 0 || child.charAt(0) == '/')
            return child;
        if (baseLength == 1 && base.charAt(0) == '/') {
            return '/' + child;
        } else {
            return base + '/' + child;
        }
    }

    public static int[] offsets(String path){
        int count, index;
        // count names
        count = 0;
        index = 0;
        if(path.isEmpty()){
            // empty path has one name
            count = 1;
        } else {
            while(index < path.length()){
                char c = path.charAt( index++ );
                if(c != '/'){
                    count++;
                    while(index < path.length() && path.charAt( index ) != '/'){
                        index++;
                    }
                }
            }
        }

        // populate offsets
        int[] offsets = new int[count];
        count = 0;
        index = 0;
        while(index < path.length()){
            char c = path.charAt( index );
            if(c == '/'){
                index++;
            } else {
                offsets[count++] = index++;
                while(index < path.length() && path.charAt( index ) != '/'){
                    index++;
                }
            }
        }
        return offsets;
    }
    
    public static boolean isAbsolute(String path) {
        return (path.length() > 0 && path.charAt(0) == '/');
    }
    
    public static String normalize(String path, int[] offsets){
        final int count = offsets.length;
        if (count == 0)
            return path;

        boolean[] ignore = new boolean[count];      // true => ignore name
        int[] size = new int[count];                // length of name
        int remaining = count;                      // number of names remaining
        boolean hasDotDot = false;                  // has at least one ..
        boolean isAbsolute = path.startsWith( "/");

        // first pass:
        //   1. compute length of names
        //   2. mark all occurences of "." to ignore
        //   3. and look for any occurences of ".."
        for (int i=0; i<count; i++) {
            int begin = offsets[i];
            int len;
            if (i == (offsets.length-1)) {
                len = path.length() - begin;
            } else {
                len = offsets[i+1] - begin - 1;
            }
            size[i] = len;

            if (path.charAt(begin) == '.') {
                if (len == 1) {
                    ignore[i] = true;  // ignore  "."
                    remaining--;
                }
                else {
                    if (path.charAt(begin+1) == '.')   // ".." found
                        hasDotDot = true;
                }
            }
        }

        // multiple passes to eliminate all occurences of name/..
        if (hasDotDot) {
            int prevRemaining;
            do {
                prevRemaining = remaining;
                int prevName = -1;
                for (int i=0; i<count; i++) {
                    if (ignore[i])
                        continue;

                    // not a ".."
                    if (size[i] != 2) {
                        prevName = i;
                        continue;
                    }

                    int begin = offsets[i];
                    if (path.charAt(begin) != '.' || path.charAt(begin+1) != '.') {
                        prevName = i;
                        continue;
                    }

                    // ".." found
                    if (prevName >= 0) {
                        // name/<ignored>/.. found so mark name and ".." to be
                        // ignored
                        ignore[prevName] = true;
                        ignore[i] = true;
                        remaining = remaining - 2;
                        prevName = -1;
                    } else {
                        // Case: /<ignored>/.. so mark ".." as ignored
                        if (isAbsolute) {
                            boolean hasPrevious = false;
                            for (int j=0; j<i; j++) {
                                if (!ignore[j]) {
                                    hasPrevious = true;
                                    break;
                                }
                            }
                            if (!hasPrevious) {
                                // all proceeding names are ignored
                                ignore[i] = true;
                                remaining--;
                            }
                        }
                    }
                }
            } while (prevRemaining > remaining);
        }

        // no redundant names
        if (remaining == count)
            return path;

        // corner case - all names removed
        if (remaining == 0) {
            return isAbsolute ? "/" : "";
        }

        // compute length of result
        int len = remaining - 1;
        if (isAbsolute) 
            len++;
        for (int i=0; i<count; i++) {
            if (!ignore[i])
                len += size[i];
        }
        
        StringBuilder sb = new StringBuilder(len);
        // copy names into result
        if (isAbsolute){
            sb.append("/");
        }
        for (int i=0; i<count; i++) {
            if (!ignore[i]) {
                int start = offsets[i];
                int stop = start + size[i];
                sb.append(path.substring(start, stop));
                if (--remaining > 0) {
                    sb.append( "/");
                }
            }
        }
        return sb.toString();
    }

    public static String normalizeSeparators(String input) {
        if(input == null || input.length() == 0)
            return "";
        int len = input.length();
        int n = len;
        // Trim the tail
        while ((n > 0) && (input.charAt(n - 1) == '/')) n--;
        if (n == 0) // we trimmed to root
            return "/";
        StringBuilder sb = new StringBuilder(input.length());
        char prevChar = 0;
        for (int i=0; i < n; i++) {
            char c = input.charAt(i);
            if ((c == '/') && (prevChar == '/'))
                continue;
            checkValidChar(input, c);
            sb.append(c);
            prevChar = c;
        }
        return sb.toString();
    }
    
    private static void checkValidChar(String input, char c) {
        if (c == '\u0000')
            throw new InvalidPathException(input, "Nul character not allowed");
    }
}
