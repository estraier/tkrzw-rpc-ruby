# Makefile for Tkrzw-RPC for Ruby

PACKAGE = tkrzw-rpc-ruby
VERSION = 0.1.2
PACKAGEDIR = $(PACKAGE)-$(VERSION)
PACKAGETGZ = $(PACKAGE)-$(VERSION).tar.gz

RUBY = ruby
RUNENV = LD_LIBRARY_PATH=.:/lib:/usr/lib:/usr/local/lib:$(HOME)/lib

all :
	echo $(PYTHON) setup.py install
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Ready to install.\n'
	@printf '#================================================================\n'

clean :
	rm -rf casket casket* *~ *.tmp *.tkh *.tkt *.tks *.flat *.log *.so \
	  hoge moge tako ika uni tkrzw_rpc/*~

install :
	echo $(PYTHON) setup.py install
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Thanks for using Tkrzw-RPC for Ruby.\n'
	@printf '#================================================================\n'

uninstall :
	echo $(PYTHON) setup.py install --record files.tmp
	xargs rm -f < files.tmp

dist :
	$(MAKE) distclean
	rm -Rf "../$(PACKAGEDIR)" "../$(PACKAGETGZ)"
	cd .. && cp -R tkrzw-rpc-ruby $(PACKAGEDIR) && \
	  tar --exclude=".*" -cvf - $(PACKAGEDIR) | gzip -c > $(PACKAGETGZ)
	rm -Rf "../$(PACKAGEDIR)"
	sync ; sync

distclean : clean apidocclean

check :
	$(RUNENV) $(RUBY) test.rb -v
	$(RUNENV) $(RUBY) perf.rb --iter 10000 --threads 3
	$(RUNENV) $(RUBY) perf.rb --iter 10000 --threads 3 --random
	$(RUNENV) $(RUBY) wicked.rb --iter 5000 --threads 3
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Checking completed.\n'
	@printf '#================================================================\n'

apidoc :
	$(MAKE) apidocclean
	mkdir -p tmp-doc
	sed -e 's/# \+@param \+\([a-zA-Z0-9]\+\) \+/# - <b>@param <i>\1<\/i><\/b> /' \
	  -e 's/# \+@\([a-z]\+\) \+/# - <b>@\1<\/b> /' tkrzw_rpc.rb > tmp-doc/tkrzw_rpc.rb
	cp overview.rd tmp-doc
	cd tmp-doc ; rdoc --title "Tkrzw-RPC" --main tkrzw_rpc.rb -o ../api-doc tkrzw_rpb.rb

apidocclean :
	rm -rf api-doc tmp-doc

pbrb : tkrzw_rpc.proto
	grpc_tools_ruby_protoc -I . --ruby_out=. --grpc_out=. tkrzw_rpc.proto

.PHONY: all clean install uninstall dist distclean check apidoc apidocclean pbrb

# END OF FILE
