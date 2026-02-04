import re
from urllib.parse import urlparse, urldefrag, urljoin, parse_qsl, parse_qs, urlencode
from typing import Iterable, Tuple
from bs4 import BeautifulSoup
from utils import get_logger
from collections import defaultdict
from collections import Counter
import re


MAX_HTML_BYTES = 5000000
MAX_SIGNATURE_REPEATS = 10
MIN_WORDS = 50
MAX_URL_LEN = 115

# To count the signature for similarity of pages
signature_counts = {}
logger = get_logger("CRAWLER")

found_pages = set()
longest_page, longest_length = "", 0
word_freq = defaultdict(int)
STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than",
    "that", "that's", "the", "their", "theirs", "them", "themselves", "then",
    "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've",
    "this", "those", "through", "to", "too", "under", "until", "up", "very", "was",
    "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what",
    "what's", "when", "when's", "where", "where's", "which", "while", "who",
    "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
    "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves"
}

sub_domains = defaultdict(int)


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    global longest_page, longest_length
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    found_pages.add(urldefrag(url).url)
    sub_domains[urlparse(url).hostname] += 1

    if resp.status != 200: # TODO: make this less strict?
        return []

    if resp.error: 
        return []

    raw_response = resp.raw_response
    if not raw_response: 
        return []

    html = raw_response.content
    if not html: 
        return []

    soup = BeautifulSoup(html, 'lxml')
    words = soup.get_text()
    words = re.findall("\w+(?:'\w+)?|[^\w\s]", words)
    for word in words: 
        if word not in STOP_WORDS: 
            word_freq[word] += 1

    if len(words) == 0: # no information
        return []

    # TODO: infinite traps

    value_score = len(words)/len(html)

    if len(html) > MAX_HTML_BYTES: # low value pages
        logger.info(f"ignoring url {url} with page size: {len(resp.raw_response.content)}")
        return []
    if len(html) > MAX_HTML_BYTES/2 and value_score < 0.05: # low value pages
        logger.info(f"ignoring url {url} with page size: {len(resp.raw_response.content)}, value_score: {value_score}")
        return []

    if len(words) > longest_length: 
        longest_length = len(words)
        longest_page = url
    

    urls = []
    for link in soup.find_all('a'):
        next_url = link.get('href')
        if "nofollow" in link.get("rel", []): # avoid share links
            continue
        if next_url: 
            abs_url = urljoin(url, next_url)
            norm_url = normalize_url(abs_url)
            if norm_url is not None:
                urls.append(norm_url)
            else: 
                logger.info(f"invalid url not appended: {next_url}")
        # print(link.get('href'))
    
    # word_count = len(words)   # To determine the information

    # signature = " ".join(words[:200])
    # if similarity_compare(signature): # Build similarity signature/report
    #     return []               

    # if word_count < MIN_WORDS:
    #     return []
    return urls

# O(n) where n is the length of the token string
def format_alphanum(token):
    current = -1
    formatted_token = []
    for i in range(len(token)):
        if not token[i].isalnum():
            formatted_token.append(token[current+1:i].lower())
            current = i
    formatted_token.append(token[current+1:].lower())
    return formatted_token

def extract_text(html: bytes) -> Iterable[Tuple[str, str]]:
    # creates a BeautifulSoup object that helps parse html beautifully
    # make sure to run {pip install beautifulsoup4}
    parser = BeautifulSoup(html, "html.parser")
    # we parse through each item in the html

    do_not_parse = {'style', 'title', '[document]', 'script', 'meta', 'head'}

    for item in parser.body.find_all(True):
        # ensures that we DO NOT PARSE through potential style objects or javascript
        if item.name in do_not_parse:
            continue
        # we will only parse text from the parent once bc of recursive=False
        text = "".join(item.find_all(string=True, recursive=False)).strip()
        if (item.name == 'a' and item.get("href")):
            yield item.get("href"), "URL"
        if (text):
            # split text into tokens (split on whitespace)
            tokens = format_alphanum(text)
            # list to store formatted tokens where token is first converted to lowercase then made into a Token object            
            for token in tokens:
                if token:
                    yield token, "word"
    # Parses the html
    # Yields a stream of tokens of either words or URLS with an identifier constructed as Tuple
    # EX: ("hello", "word"), ("www..ics.uci.edu/", "URL")
    

def similarity_compare(signature: str)-> bool:
    # Stores signatures of Pages into a Dictionary
    # If signature count reaches threshold, Don't extract URL from page
    if signature in signature_counts:
        signature_counts[signature] += 1
    else:
        signature_counts[signature] = 1
    
    return signature_counts[signature] > MAX_SIGNATURE_REPEATS

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        if len(url) > MAX_URL_LEN: # potential crawler trap: URL getting longer
            return False
        parsed = urlparse(url)
        if "/events/" in parsed.path: # calendar trap TODO: is this really though? https://connectedlearning.uci.edu/events/
            logger.info(f"detected and skipped calendar trap at: {url}")
            return False
        if "do" in parse_qs(parsed.query): # do=_ query trap
            logger.info(f"detected and skipped do=_ query trap at: {url}")
            return False
        if parsed.scheme not in set(["http", "https"]):
            return False
        allowed = {"ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"}
        if not any(parsed.hostname == d or parsed.hostname.endswith("." + d) for d in allowed):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def normalize_url(url): 
    try:
        p = urlparse(url)
    except ValueError:
        return None

    if p.scheme not in ("http", "https"):
        return None

    if p.hostname is None:
        return None

    new_url = f"{p.scheme}://{p.hostname}"
    
    if p.port and p.port not in (80, 443):
        new_url += f":{p.port}"

    new_url += p.path
    if p.query:
        # handling C=_;O=_ trap
        query = p.query.replace(";", "&")
        kept = [
            (k, v) for (k, v) in parse_qsl(query, keep_blank_values=True)
            if k.lower() not in {"c", "o"}
        ]
        new_query = urlencode(kept, doseq=True)
        new_url += f"?{new_query}"

    return new_url

def finish(): # print out statistics
    logger.info(f"unique pages: {len(found_pages)}")
    logger.info(f"longest page: {longest_page}, length: {longest_length}")
    logger.info(f"most common 50: {Counter(word_freq).most_common(50)}")
    logger.info(f"subdomains: ")
    for key, value in sorted(subdomains.items()): 
        logger.info(f"{key}, {value}")

