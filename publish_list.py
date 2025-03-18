import hashlib
import logging
import os
import requests
import sys
import time
import urllib.parse

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


CONFIG_TEMPLATE = '''
admin_passwd        =   {list_name}.passwd
administrivia       =   no
advertise           <<  END

END
approve_passwd      =   {list_name}.passwd
archive_dir         =
comments            <<  END

END
date_info           =   yes
debug               =   no
description         =   {list_description}
digest_archive      =
digest_issue        =   1
digest_name         =   {list_name}
digest_rm_footer    =
digest_rm_fronter   =
digest_volume       =   1
digest_work_dir     =
maxlength           =   {size}
message_footer      <<  END

END
message_fronter     <<  END

END
message_headers     <<  END

END
moderate            =   no
mungedomain         =   no
noadvertise         <<  END
/.*/
END
precedence          =   list
private_get         =   yes
private_index       =   yes
private_info        =   yes
private_which       =   yes
private_who         =   yes
purge_received      =   no
reply_to            =
resend_host         =
restrict_post       =   {authusers_filename}
sender              =   owner-{list_name}
strip               =   yes
subject_prefix      =
subscribe_policy    =   closed
non_member_bounce   =   sender-owner
'''

ALIASES_TEMPLATE = '''
{list_name}: "|/opt/majordomo/wrapper resend -l {list_name} -h Fuqua.Duke.Edu {list_name}-{obfuscator}"
{list_name}-{obfuscator}: :include:/opt/majordomo/lists/{list_name}
owner-{list_name}: {list_owners}
{list_name}-request: "|/opt/majordomo/wrapper request-answer {list_name}"
{list_name}-approval: owner-{list_name}
'''


def fetch_joe_api_object(url, key):
    res = requests.get(url, timeout=30)
    res.raise_for_status()
    payload = res.json()
    assert payload['success'], 'API call to {} failed: {}'.format(url, payload)
    return payload.get(key)


def get_list_name(list_info):
    assert 'mail' in list_info, 'no mail attribute to get list name from: {}'.format(list_info)
    return list_info['mail'].split('@')[0].lower()


def get_list_filename(list_info, extension=None):
    list_filename = get_list_name(list_info)

    if extension:
        list_filename = '.'.join([list_filename, extension])

    return list_filename


def get_owner_addresses(dn):
    addresses = set()

    recipient_info = fetch_joe_api_object(
        'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/dn/{dn}'.format(
            dn=urllib.parse.quote(dn)
        ),
        'user',
    )
    logger.debug('recipient info for %s: %s', dn, recipient_info)

    if 'fuquagroup' in recipient_info['objectclass']:
        recipient_group_info = fetch_joe_api_object(
            'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/mail/recipients/{dn}'.format(
                dn=urllib.parse.quote(dn)
            ),
            'users',
        )
        logger.debug('group recipient info %s: %s', dn, recipient_group_info)

        addresses.update(users_to_addresses(recipient_group_info))
    else:
        if 'mail' in recipient_info:
            addresses.add(recipient_info['mail'])

    return addresses


def get_sender_addresses(primary_senders):
    valid_domains = ['duke.edu', 'mail.duke.edu', 'acpub.duke.edu', 'win.duke.edu']
    extra_attributes = ['acpubmail', 'x121address']

    sender_addresses = set()

    for sender in primary_senders:
        mail = sender.get('mail')
        if not mail:
            logger.warning('no mail attribute for %s, skipping', sender)
            continue
        sender_addresses.add(mail)

        netid = sender.get('netid', sender.get('uid'))
        if not netid:
            logger.error('no netid or uid for %s, skipping', sender)
            continue

        for domain in valid_domains:
            sender_addresses.add('@'.join([netid, domain]))

        for attr_name in extra_attributes:
            attr_val = sender.get(attr_name)
            if attr_val:
                sender_addresses.add(attr_val)

    return sender_addresses


def get_obfuscator(list_name):
    digest = hashlib.sha256()
    digest.update(time.asctime().encode())
    digest.update(str(os.getpid()).encode())
    digest.update(list_name.encode())
    return digest.hexdigest()


def write_config(config, list_info, authusers_filename=None):
    list_name = get_list_name(list_info)
    config_filename = get_list_filename(list_info, 'config')
    passwd_filename = get_list_filename(list_info, 'passwd')

    with open(os.path.join(config.output, config_filename), "w") as output_file:
        output_file.write(CONFIG_TEMPLATE.format(
            list_name=list_name,
            list_description=list_info.get('description', list_info.get('displayName', '')),
            authusers_filename=authusers_filename or '',
            **list_info
        ))

    with open(os.path.join(config.output, passwd_filename), 'w') as passwd_file:
        passwd_file.write('nopassword\n')

    return config_filename


def write_aliases(config, list_info):
    list_owners = {'fuqua.major-owne6579@win.duke.edu'}

    # A list's "owner" is who gets bounces so see if that's overridden by a "bouncer"
    for owner_attribute in ['bouncer', 'owner']:
        if owner_attribute in list_info:
            owner_dn = list_info[owner_attribute]
            list_owners.update(get_owner_addresses(owner_dn))
            break

    logger.debug('list owners %s', list_owners)

    list_owners.discard('jamesc@duke.edu')
    logger.debug('list owners minus myself %s', list_owners)

    list_name = get_list_name(list_info)
    aliases_filename = get_list_filename(list_info, 'aliases')

    with open(os.path.join(config.output, aliases_filename), "w") as output_file:
        output_file.write(ALIASES_TEMPLATE.format(
            list_name=list_name,
            list_owners=','.join(list_owners),
            obfuscator=get_obfuscator(list_name),
            **list_info
        ))

    return aliases_filename


def output_addresses(addresses, config, list_info, extension=None):
    list_name = get_list_filename(list_info, extension)

    with open(os.path.join(config.output, list_name), "w") as output_file:
        output_file.writelines(map(lambda x: x + '\n', addresses))

    return list_name


def users_to_addresses(users):
    addresses = set()

    for user in users:
        mail = user.get('mail')
        if mail:
            addresses.add(mail)
        else:
            logger.warning('no mail attribute on %s, skipping', user)

    return addresses


def write_recipients(config, list_info):
    primary_recipients = fetch_joe_api_object(
        'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/mail/recipients/{dn}'.format(
            dn=urllib.parse.quote(list_info['dn'])
        ),
        'users',
    )
    logger.debug('primary recipients %s', primary_recipients)

    primary_recipient_addresses = users_to_addresses(primary_recipients)
    logger.debug('primary recipient addresses %s', primary_recipient_addresses)

    primary_recipient_addresses.update(list_info.get('altmail', []))
    logger.debug('primary recipient addresses plus altmail %s', primary_recipient_addresses)

    return output_addresses(primary_recipient_addresses, config, list_info)


def write_senders(config, list_info):
    # There is no <list>.authusers file if the list is public (default is not public)
    if list_info.get('public', 'false') == 'true':
        return None

    primary_senders = fetch_joe_api_object(
        'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/senders/{dn}'.format(
            dn=urllib.parse.quote(list_info['dn'])
        ),
        'users',
    )
    logger.debug('primary senders %s', primary_senders)

    primary_sender_addresses = get_sender_addresses(primary_senders)
    logger.debug('primary sender addresses %s', primary_sender_addresses)

    primary_sender_addresses.update(list_info.get('altauthmail', []))
    logger.debug('primary sender addresses plus altauthmail %s', primary_sender_addresses)

    return output_addresses(primary_sender_addresses, config, list_info, 'authusers')


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    # parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser = ArgumentParser()
    #parser.add_argument('-d', '--debug', action='store_true', default=True,
    #                    help='display debug messages')
    # parser.add_argument('-o', '--output', default='.',
    #                     help='directory to write files into')
    # parser.add_argument('list_dn', help='DN of the list in LDAP')
    parser.add_argument('-o', '--output')
    parser.add_argument('-dn', '--list_dn')

    args = parser.parse_args(argv)

    # if args.debug:
    #     logger.setLevel(logging.DEBUG)
    # else:
    #     logger.setLevel(logging.INFO)

    logger.info(str(args))
    logger.info("args: " + "output -> " + str(args.output))
    logger.info("args: " + "list_dn -> " + str(args.list_dn))

    list_info = fetch_joe_api_object(
        'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/{dn}'.format(
            dn=urllib.parse.quote(args.list_dn.strip())
        ),
        'group',
    )
    #logger.info('list info %s', list_info)
    for k,v in list_info.items():
        logger.info("list_info: " + k + " -> " + str(v))

    if list_info['publish'] == 'true':
        write_recipients(args, list_info)
        authusers_filename = write_senders(args, list_info)
        write_config(args, list_info, authusers_filename)
        write_aliases(args, list_info)

def set_up_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
 
    # this is the dir where subscribe.py is running
    # e.g. /home/win.duke.edu/lcc9/projects/pubsub/subscribe/redis_subscribe
    # (subscribe.py calls src/services/redis_subscribe.py, which calls the doIt.sh bash script, which calls publish_list.py)
    current_directory = os.getcwd() 
    log_file_name = os.path.join(current_directory, "major_update.log") 

    file_handler = logging.FileHandler(log_file_name)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(filename)s %(funcName)s %(lineno)s: %(message)s")
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

if __name__ == "__main__":
    # logging.basicConfig(format='%(levelname)s:%(filename)s:%(lineno)d\t"%(message)s"')
    # logging.basicConfig(
    #     filename='app.log',
    #     level=logging.INFO,
    #     format='%(levelname)s:%(filename)s:%(lineno)d\t"%(message)s"'
    # )
    # logger = logging.getLogger(__name__)

    logger = set_up_logger()
    # logger.info("hello")
    # logger.info(str(os.getcwd()))
    sys.exit(main())
