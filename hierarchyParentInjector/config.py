


# Simple user feedback, to remind/express/nag them for turning off things that
# they probably shouldn't (but may sometimes need to)
def educate(subject):

    if subject == "abortOnIssues":
        print("/n WARNING !!/n. Aborting on encountering an issue is turned off via config.py. This should NEVER be turned off without " +
              + "good reason as this is your principle defence againt data pollution.")


# Config object because globals are satan
class config(object):

    def __init__(self):
        self.abortOnIssues = True

        # Warn the user if they're living dangerously
        if not self.abortOnIssues:
            educate("abortOnIssues")



cfg = config()
