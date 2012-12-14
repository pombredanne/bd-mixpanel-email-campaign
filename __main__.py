from bitdeli.chain import Profiles
from bitdeli.widgets import Text

def newest(prop):
    if prop:
        return max((iter(hours).next(), value)
                   for value, hours in prop.iteritems())[1]

def begin(profiles):
    for profile in profiles:
        props = profile['properties']
        if 'email' in props:
            yield profile, {'email': newest(props['email'])}
                
def end(profiles):
    yield [columns for profile, columns in profiles]
                
def campaign(profiles):
    for profile, columns in profiles:
        campaign = newest(profile['properties'].get('utm_campaign', {}))
        if campaign:
            columns['campaign'] = campaign           
        yield profile, columns

def datasources(profiles):
    for profile, columns in profiles:
        for input in profile['properties'].get('input_type', {}):
            if 'toydata' not in input:
                columns['data'] = input
        yield profile, columns
        
def repos(profiles):
    for profile, columns in profiles:
        for repo in profile['properties'].get('repo', {}):
            if 'toydata' not in repo:
                columns['repo'] = repo
        yield profile, columns
        
Profiles().map(begin)\
          .map(campaign)\
          .map(datasources)\
          .map(repos)\
          .map(end)\
          .show('table',
                id='email-data',
                size=(12, 6),
                csv_export=True)  
    
