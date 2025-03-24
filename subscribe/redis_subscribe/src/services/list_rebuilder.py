import hashlib
import logging
import os
import requests
import sys
import time
import urllib.parse

#from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from src.logging.app_logger import AppLogger

class ListRebuilder(object):

    def __init__(self, list_name, list_work_directory, dn):
        self.logger = AppLogger.get_logger()

        self.list_name = list_name
        self.list_work_directory = list_work_directory
        self.dn = dn
        self.logger.info("dn: " + str(self.dn))
        self.logger.info("list name: " + str(self.list_name))
        self.logger.info("list work directory: " + str(self.list_work_directory))

        self.CONFIG_TEMPLATE = '''
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

        self.ALIASES_TEMPLATE = '''
            {list_name}: "|/opt/majordomo/wrapper resend -l {list_name} -h Fuqua.Duke.Edu {list_name}-{obfuscator}"
            {list_name}-{obfuscator}: :include:/opt/majordomo/lists/{list_name}
            owner-{list_name}: {list_owners}
            {list_name}-request: "|/opt/majordomo/wrapper request-answer {list_name}"
            {list_name}-approval: owner-{list_name}
            '''

        list_info = self.fetch_joe_api_object(
            'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/{dn}'.format(
                dn=urllib.parse.quote(self.dn.strip())
            ),
            'group',
        )

        #for k,v in list_info.items():
        #    self.logger.info("list_info: " + k + " -> " + str(v))

        if list_info['publish'] == 'true':
            self.write_recipients(list_info)
            authusers_filename = self.write_senders(list_info)
            #self.logger.info("authusers_filename is " + str(authusers_filename))
            config_filename = self.write_config(list_info, authusers_filename)
            self.write_aliases(list_info)

    def fetch_joe_api_object(self, url, key):
        self.logger.info("URL: " + str(url))
        #self.logger.info("key: " + str(key))
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        payload = res.json()
        assert payload['success'], 'API call to {} failed: {}'.format(url, payload)
        return payload.get(key)

    # def get_list_name(self, list_info):
    #     assert 'mail' in list_info, 'no mail attribute to get list name from: {}'.format(list_info)
    #     return list_info['mail'].split('@')[0].lower()

    def get_list_filename(self, list_info, extension=None):
        #list_filename = self.get_list_name(list_info)
        list_filename = self.list_name

        if extension:
            list_filename = '.'.join([list_filename, extension])

        return list_filename

    def get_owner_addresses(self, dn):
        addresses = set()

        recipient_info = self.fetch_joe_api_object(
            'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/dn/{dn}'.format(
                dn=urllib.parse.quote(dn)
            ),
            'user',
        )
        #self.logger.info('recipient info for %s: %s', dn, recipient_info)
        #for k,v in recipient_info.items():
        #    self.logger.info("recipient info: " + k + " -> " + str(v))

        if 'fuquagroup' in recipient_info['objectclass']:
            recipient_group_info = self.fetch_joe_api_object(
                'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/mail/recipients/{dn}'.format(
                    dn=urllib.parse.quote(dn)
                ),
                'users',
            )
            #self.logger.info('group recipient info %s: %s', dn, recipient_group_info)
            #for k,v in recipient_group_info.items():
            #    self.logger.info("recipient group info: " + k + " -> " + str(v))

            addresses.update(self.users_to_addresses(recipient_group_info))
        else:
            if 'mail' in recipient_info:
                addresses.add(recipient_info['mail'])

        return addresses

    def get_sender_addresses(self, primary_senders):
        valid_domains = ['duke.edu', 'mail.duke.edu', 'acpub.duke.edu', 'win.duke.edu']
        extra_attributes = ['acpubmail', 'x121address']

        sender_addresses = set()

        for sender in primary_senders:
            mail = sender.get('mail')
            if not mail:
                self.logger.warning('no mail attribute for %s, skipping', sender)
                continue
            sender_addresses.add(mail)

            netid = sender.get('netid', sender.get('uid'))
            if not netid:
                self.logger.error('no netid or uid for %s, skipping', sender)
                continue

            for domain in valid_domains:
                sender_addresses.add('@'.join([netid, domain]))

            for attr_name in extra_attributes:
                attr_val = sender.get(attr_name)
                if attr_val:
                    sender_addresses.add(attr_val)

        return sender_addresses

    def get_obfuscator(self, list_name):
        digest = hashlib.sha256()
        digest.update(time.asctime().encode())
        digest.update(str(os.getpid()).encode())
        digest.update(list_name.encode())
        return digest.hexdigest()

    def write_config(self, list_info, authusers_filename=None) -> str:
        #self.logger.info("list name is " + str(self.list_name))
        config_filename = self.get_list_filename(list_info, 'config')
        passwd_filename = self.get_list_filename(list_info, 'passwd')

        # with open(os.path.join(config.output, config_filename), "w") as output_file:
        with open(os.path.join(self.list_work_directory, config_filename), "w") as output_file:
            output_file.write(self.CONFIG_TEMPLATE.format(
                list_name=self.list_name,
                list_description=list_info.get('description', list_info.get('displayName', '')),
                authusers_filename=authusers_filename or '',
                **list_info
            ))

        self.logger.info("Created: " + str(os.path.join(self.list_work_directory, config_filename)))

        with open(os.path.join(self.list_work_directory, passwd_filename), 'w') as passwd_file:
            passwd_file.write('nopassword\n')

        self.logger.info("Created: " + str(os.path.join(self.list_work_directory, passwd_filename)))
        return config_filename

    # def write_aliases(self, config, list_info):
    def write_aliases(self, list_info):
        list_owners = {'fuqua.major-owne6579@win.duke.edu'}

        # A list's "owner" is who gets bounces so see if that's overridden by a "bouncer"
        for owner_attribute in ['bouncer', 'owner']:
            if owner_attribute in list_info:
                owner_dn = list_info[owner_attribute]
                list_owners.update(self.get_owner_addresses(owner_dn))
                break

        self.logger.info('list owners %s', list_owners)

        list_owners.discard('jamesc@duke.edu')
        #self.logger.info('list owners minus myself %s', list_owners)

        #list_name = self.get_list_name(list_info)
        aliases_filename = self.get_list_filename(list_info, 'aliases')

        # with open(os.path.join(config.output, aliases_filename), "w") as output_file:
        with open(os.path.join(self.list_work_directory, aliases_filename), "w") as output_file:
            output_file.write(self.ALIASES_TEMPLATE.format(
                list_name=self.list_name,
                list_owners=','.join(list_owners),
                obfuscator=self.get_obfuscator(self.list_name),
                **list_info
            ))

        self.logger.info("Created: " + str(os.path.join(self.list_work_directory, aliases_filename)))

        return aliases_filename


    # def output_addresses(self, addresses, config, list_info, extension=None):
    def output_addresses(self, addresses, list_info, extension=None):
        list_name = self.get_list_filename(list_info, extension)

        # with open(os.path.join(config.output, list_name), "w") as output_file:
        with open(os.path.join(self.list_work_directory, list_name), "w") as output_file:
            output_file.writelines(map(lambda x: x + '\n', addresses))

        #self.logger.info(str(output_file) + " has been created.  It contains:")
        #for addr in addresses:
        #    self.logger.info(str(addr))
        self.logger.info("Created: " + str(os.path.join(self.list_work_directory, list_name)))

        return list_name


    def users_to_addresses(self, users) -> set:
        addresses = set()

        for user in users:
            mail = user.get('mail')
            if mail:
                addresses.add(mail)
            else:
                self.logger.warning('no mail attribute on %s, skipping', user)

        return addresses


    #def write_recipients(self, config, list_info):
    def write_recipients(self, list_info) -> str:
        # primary_recipients is data type list of dictionaries
        primary_recipients = self.fetch_joe_api_object(
            'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/mail/recipients/{dn}'.format(
                dn=urllib.parse.quote(list_info['dn'])
            ),
            'users',
        )
        #self.logger.info('primary recipients %s', primary_recipients)
        for pr in primary_recipients:
            self.logger.info("recipient: " + str(pr))

        primary_recipient_addresses = self.users_to_addresses(primary_recipients) # data type is set
        #self.logger.info('primary recipient addresses %s', primary_recipient_addresses)
        #for pra in primary_recipient_addresses:
        #    self.logger.info("primary recipient address: " + str(pra))

        primary_recipient_addresses.update(list_info.get('altmail', []))
        #self.logger.info('primary recipient addresses plus altmail %s', primary_recipient_addresses)
        #for pra in primary_recipient_addresses:
        #    self.logger.info("primary recipient address plus altmail: " + str(pra))

        # return self.output_addresses(primary_recipient_addresses, config, list_info)
        return self.output_addresses(primary_recipient_addresses, list_info)


    # def write_senders(self, config, list_info):
    def write_senders(self, list_info):
        #self.logger.info(str(list_info))
        # There is no <list>.authusers file if the list is public (default is not public)
        if list_info.get('public', 'false') == 'true':
            self.logger.info(self.list_name + ".authusers file will not be created because this list is public.")
            return None

        primary_senders = self.fetch_joe_api_object(
            'https://go.fuqua.duke.edu/fuqua_link/rest/ldap/senders/{dn}'.format(
                dn=urllib.parse.quote(list_info['dn'])
            ),
            'users',
        )
        self.logger.info(str(type(primary_senders)))
        self.logger.info('primary senders %s', primary_senders)

        primary_sender_addresses = self.get_sender_addresses(primary_senders)
        self.logger.info(str(type(primary_sender_addresses)))
        self.logger.info('primary sender addresses %s', primary_sender_addresses)

        primary_sender_addresses.update(list_info.get('altauthmail', []))
        self.logger.info('primary sender addresses plus altauthmail %s', primary_sender_addresses)

        # return self.output_addresses(primary_sender_addresses, config, list_info, 'authusers')
        return self.output_addresses(primary_sender_addresses, list_info, 'authusers')
