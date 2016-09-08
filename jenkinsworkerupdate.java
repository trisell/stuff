import jenkins.*
import jenkins.model.*
import hudson.*
import hudson.model.*

def list = manager.build.logFile.readLines()
def ami = list =~ /ami-[0-9 a-z]{8}/

def inst = Jenkins.getInstance()
def template = inst.clouds[0].templates[0]
template.setAmi(ami[-1])
inst.save()
