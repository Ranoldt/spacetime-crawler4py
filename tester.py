import re
from urllib.parse import urlparse
import scraper 

# Care about:
    #   *.ics.uci.edu/*
    #   *.cs.uci.edu/*  
    #   *.informatics.uci.edu/* 
    #   *.stat.uci.edu/*

# Parts of url from parsed = urlparse(url):
        # scheme: "https" - the protocol
        # netloc: "www.ics.uci.edu" - the network location (domain/hostname)
        # path: "/~faculty/profile.html" - the path on the server
        # params: "" - parameters (rarely used nowadays)
        # query: "id=123" - the query string after ?
        # fragment: the fragment/anchor after #

def test_is_valid(url, truth) :
    # parsed = urlparse(url)
    # print("netloc: ", parsed.netloc)
    print(scraper.is_valid(url) == truth)
    # print()

# Test:
print("Does is_valid return the right T/F label")
test_is_valid("hello://darkness.my.old.friend", False)
test_is_valid("https://ics.uci.edu/", True)             
test_is_valid("https://stat.ics.uci.edu/k", True) 
test_is_valid("https://ics.uci.edu:8080/", False) 
test_is_valid("https://www.informatics.uci.edu/research/example-research-projects/", True) # NOPE

