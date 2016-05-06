package com.flume.source.dirwatchdog;

import java.io.File;
import java.io.FileFilter;
import java.util.regex.Pattern;


public class RegexFileFilter implements FileFilter {
    Pattern p; // not thread safe

    public RegexFileFilter(String regex) {
        this.p = Pattern.compile(regex);
    }

    @Override
    public boolean accept(File pathname) {
        return p.matcher(pathname.getName()).matches();
    }

    public static void main(String[] args) {
        Pattern ignorePattern = Pattern.compile("[a-z]{0,5}-\\d{0,2}\\..*");
        boolean t = ignorePattern.matcher("group-1.2014-12-17").matches();
//         t = ignorePattern.matcher("group-1.").matches();
        System.out.println(t);
    }
}
