import os
import email
import cPickle
import getpass
import imaplib
import datetime

IMAP_SERVER = 'imap.gmail.com'
def process_inbox(mail, email_address):
   return_value, emails = mail.search(None, '(SINCE "31-May-2017" BEFORE "01-Jun-2017" FROM "dcscm-no-reply@amadeus.com")' ) #'(FROM "dcscm-no-reply@amadeus.com")'

   #M.search(None, '(SINCE "01-Jan-2012")')
   #M.search(None, '(BEFORE "01-Jan-2012")')
   #M.search(None, '(SINCE "01-Jan-2012" BEFORE "02-Jan-2012")')

   if return_value != 'OK':
      print "No messages."
      return

   if os.path.exists('./pickled_emails.pkl'):
      pickled_emails = open('./pickled_emails.pkl', 'r')
      older_emails = cPickle.load(pickled_emails)
      pickled_emails.close()
      new_emails = [my_mail for my_mail in emails[0].split() if my_mail not in older_emails]
      try:
         for my_mail in new_emails:
            return_value, raw_data = mail.fetch(my_mail, '(RFC822)')
            if return_value != 'OK':
               print "Error getting message", my_mail
               return

            message = email.message_from_string(raw_data[0][1])
            date_tuple = email.utils.parsedate_tz(message['Date'])
            local_date = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
            formatted_date = local_date.strftime("%d %b %Y %H:%M:%S")
            for part in message.walk():
               if part.get_content_type() == "text/plain":
                  body = part.get_payload(decode=True)
                  save_string = str("./" + email_address + "_data" + "/" + str(my_mail) + ".(" + str(formatted_date) + ")-" + str(message['Subject']).replace("/", "-") + ".txt")
                  myfile = open(save_string, 'a')
                  myfile.write(body)
                  myfile.close()
               else:
                  continue
            older_emails.append(my_mail)
         pickled_emails = open("./pickled_emails.pkl", 'w')
         cPickle.dump(older_emails, pickled_emails)
         pickled_emails.close()
      except:
         pickled_emails = open("./pickled_emails.pkl", 'w')
         cPickle.dump(older_emails, pickled_emails)
         pickled_emails.close()
   else:
      stored_emails = list()
      try:
         for my_mail in emails[0].split():
            return_value, raw_data = mail.fetch(my_mail, '(RFC822)')
            if return_value != 'OK':
               print "Error getting message", my_mail
               return

            message = email.message_from_string(raw_data[0][1])
            date_tuple = email.utils.parsedate_tz(message['Date'])
            local_date = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
            formatted_date = local_date.strftime("%d %b %Y %H:%M:%S")
            for part in message.walk():
               if part.get_content_type() == "text/plain":
                  body = part.get_payload(decode=True)
                  save_string = str("./" + email_address + "_data" + "/" + str(my_mail) + ".(" + str(formatted_date) + ")-" + str(message['Subject']) + ".txt")
                  myfile = open(save_string, 'a')
                  myfile.write(body)
                  myfile.close()
               else:
                  continue
            stored_emails.append(my_mail)
         pickled_emails = open("./pickled_emails.pkl", 'w')
         cPickle.dump(stored_emails, pickled_emails)
         pickled_emails.close()
      except:
         pickled_emails = open("./pickled_emails.pkl", 'w')
         cPickle.dump(stored_emails, pickled_emails)
         pickled_emails.close()

def get_inbox(mail):
   return_value, inbox_mail = mail.select("INBOX")
   return return_value


def main():
   mail = imaplib.IMAP4_SSL(IMAP_SERVER)
   return_value = 0
   while return_value == 0:
      email_address = raw_input('Email:')
      try:
         return_value, data = mail.login(email_address, getpass.getpass())
      except imaplib.IMAP4.error:
         print "Login failed."
   if os.path.exists("./" + email_address + "_data"):
      ret = get_inbox(mail)
      if ret == "OK":
         process_inbox(mail, email_address)
         mail.close()
   else:
      os.makedirs("./" + email_address + "_data")
      ret = get_inbox(mail)
      if ret == "OK":
         process_inbox(mail, email_address)
         mail.close()


   mail.logout()

main()