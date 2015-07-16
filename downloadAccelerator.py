#!/usr/bin/env python


import argparse
import threading
import requests
from requests.auth import HTTPDigestAuth
import time
import os
from humanize import naturalsize
from urlparse import urlparse, parse_qs
requests.packages.urllib3.disable_warnings()    # when verify=False ("insecure" option)

""" Threaded download accelerator """
class Downloader:
    def __init__(self, url, filesize=None, outfile=None, threads=1, debug=False, x_headers=None, auth=None, parse_url=False, verify=True):
        """ initialize this object with the number of threads to use to 
        download a given URL """
        self.num_threads = threads
        self.debug = debug
        self.url = url
        self.out_file = 'index.html'
        if outfile:
            self.out_file = outfile
        else:
            qs = urlparse(self.url).query
            if qs:
                pqs = parse_qs(qs)
                if 'filename' in pqs:
                    self.out_file = pqs['filename'][0]
                elif 'file' in parsed_qs:
                    self.out_file = pqs['file'][0]
                elif len(qs) <= 40:
                    self.out_file = qs
            else:
                pth = urlparse(self.url).path.split('/')[-1]
                if 1 < len(pth) < 40:
                    self.out_file = pth

        # delete the output file if it already exists
        if os.path.isfile(self.out_file):
            os.remove(self.out_file)
             
        self.x_headers = x_headers       
        self.parse_url = parse_url               
        self.verify = verify      # set to "false" to "fix" pyasn1 ssl-related error
            
        if self.debug:      # this could also be a verbose option
            print "Downloader will atempt to use %s threads" % self.num_threads
            print "Downloader will download the file located at %s" % self.url            
            print "File will be downloaded as %s" % self.out_file
            print "Extra request headers: %s " % self.x_headers
            if self.parse_url:
                print "Content length will be parsed from url"
            print "Host certificate verification: %s" % self.verify
            
        if auth:
            user, pwd = auth
            self.auth = HTTPDigestAuth(user, pwd)
            if self.debug:
                print "Using Digest authentication"
        else:
            self.auth = None
        self.filesize = filesize

    def download(self):
        """ download the file at the given URL """

        if not self.filesize:
            # get filesize from a HEAD request to the URL
            if not self.parse_url:
                response = requests.head(self.url, headers=self.x_headers, auth=self.auth)
                if self.debug:
                    print
                    print "HTTP Headers:"
                    print "============="
                    for key in response.headers:
                        print key + " = " + response.headers[key]
                    print "-------------"
                    #print response.request.headers

                # should I check the response status code?
                try:
                    content_length = int(response.headers['content-length'])
                except KeyError as e:
                    self.x_headers.update({'Range': 'bytes=0-0'})
                    resp = requests.get(self.url, headers=self.x_headers, auth=self.auth)
                    cr = resp.headers['content-range'].split('/')[-1]
                    content_length = int(cr)
                    del self.x_headers['Range']                
            else:  
                # get filesize from the query string of the user-provided url
                # works only if 'fsize' is present (e.g. yandex disk)    
                parsed_qs = parse_qs(urlparse(self.url).query)
                fsize = int(parsed_qs['fsize'][0])
                content_length = int(fsize)
        else:
            content_length = int(filesize)
        if self.debug:
            print "Content length: %s bytes" % content_length

        bytes_per_thread = content_length / self.num_threads
        if self.debug:
            print "Number of bytes per thread: %s" % bytes_per_thread

        # create the download threads
        try:
            with Timer() as timer:
                threads = []
                for i in range(self.num_threads):
                    thread_file = self.out_file + "_part_" + str(i)
                    if self.debug:
                        print "Thread %d output file: %s" % (i, thread_file)

                    begin_range = i * bytes_per_thread
                    end_range = (i + 1) * bytes_per_thread - 1
                    remainder = (content_length - 1) - end_range
                    if self.debug:
                        print "Thread %d begin range at %d" % (i, begin_range)
                        print "Thread %d end range at %d" % (i, end_range)
                        print "Thread %d remainder: %d bytes" % (i, remainder)
                    if remainder < bytes_per_thread:
                        end_range += remainder
                        if self.debug:
                            print "Thread %d end range updated at %d" % (i, end_range)

                    t = DownloaderThread(self.url, begin_range, end_range, thread_file, self.x_headers, self.verify, self.auth)
                    threads.append(t)
                    t.start()

                # wait for the threads
                for t in threads:
                    t.join()
                    with open(self.out_file, 'ab') as final_out_file:
                        with open(t.out_file, 'rb') as thread_out_file:
                            # read data from thread_out_file and write to final_out_file
                            bytes = 'empty'
                            while (bytes):
                                bytes = thread_out_file.read(512*1024)
                                #bytes = thread_out_file.read() 
                                final_out_file.write(bytes)
                    
                    os.remove(t.out_file)       # delete the thread's output file

        finally:
            print "%s %d %d %f" % (self.url, self.num_threads, content_length, timer.interval)
            print "avg download speed: %s/s" % naturalsize(content_length/timer.interval, binary=True, format='%.2f')

""" Thread class used to download a portion of a file """
class DownloaderThread(threading.Thread):
    def __init__(self, url, begin_range, end_range, out_file, headers, verify, auth):
        """ initialize the thread with everything it will need to 
        download the data """
        threading.Thread.__init__(self)
        self.url = url
        self.begin_range = begin_range
        self.end_range = end_range
        self.out_file = out_file
        self.headers = headers
        self.verify = verify
        self.auth = auth

    def run(self):
        """ download the data to the given file """
        headers = {'Range' : 'bytes=%s-%s' % (self.begin_range, self.end_range)}
        headers.update(self.headers)
        response = requests.get(self.url, headers=headers, verify=self.verify, stream=True, auth=self.auth)
        #print response.request.headers     # x-debug
        with open(self.out_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=512*1024):
                f.write(chunk)

""" Timer class for computing execution times """
class Timer:
    # changed time.clock() to time.time()
    def __enter__(self):
        self.start = time.time()
        return self
    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start

if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(prog="Download Accelerator", description="Download a file specified by a given URL to the current directory or to the specified (full) path.")
    parser.add_argument('-n', '--threads', type=str, help='number of threads to use to donwload the file (default is 1)', default=1)
    parser.add_argument('-d', '--debug', action='store_true', help='Turn debug statements on')
    parser.add_argument('-o', '--outfile', help='optional outfile full path')
    parser.add_argument('-s', '--size', help='file size (if known)')
    parser.add_argument('-H', '--headers', nargs='*', help='extra request headers, e.g OAuth')
    parser.add_argument('-q', '--parse_url', action='store_true', help='parse url to get the filesize rather than using a HEAD request')
    parser.add_argument('-k', '--insecure', action='store_true', help='disable host certificate verification (workaround for pyasn1-related error)')
    parser.add_argument('-a', '--auth', metavar="username,password", help="username and password for digest auth")
    parser.add_argument('url', help='url to download')
    args = parser.parse_args()
    
    url = args.url
    if args.outfile:
        outfile=args.outfile
    threads = int(args.threads)
    if args.headers:
        headers = args.headers
    verify = True
    if args.insecure:
        verify = False
    parse_url = False
    if args.parse_url:
        parse_url = True
    extra_headers = dict()
    if args.headers:
        extra_headers.update((i.split(',')[0].strip(), i.split(',')[1].strip()) for i in args.headers)
    auth = None
    if args.auth:
        auth =list(i.strip() for i in auth.split(','))
    filesize = None
    if args.size:
        filesize = args.size
    s = time.time()
    d = Downloader(url, outfile=outfile, filesize=filesize, threads=threads, debug=args.debug, x_headers=extra_headers, auth=args.auth, parse_url=args.parse_url, verify=verify)
    d.download()
    print "total execution time: %.2f" % (time.time() - s)
