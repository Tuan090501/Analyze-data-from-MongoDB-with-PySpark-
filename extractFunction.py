import re

def extract_language(string) :
    language_regex = r"Java|Python|C\+\+|C\#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL"
    if string is not None :
        return re.findall(language_regex, string)

def extract_domain(string):
    domain_regex = r"href=\"(\S+)\""
    domains = re.findall(domain_regex, string)
    output = []
  
    for domain in domains:
        try:
            output.append(domain.split('/')[2])
        except IndexError as e:
            pass
           
    return output