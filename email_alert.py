import yagmail
from secrets import EMAIL


def send_email(to, subject, body):
	yag = yagmail.SMTP(EMAIL['user'], EMAIL['password'])
	yag.send(to = to, subject = subject, contents = body)
