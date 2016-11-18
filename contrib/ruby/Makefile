all: lib/dxruby/api.rb lib/dxruby/version.rb

contrib/ruby/lib/dxruby/version.rb:
	(if [ ! -e lib/dxruby/version.rb ]; then \
	    echo -e "module DX\n  VERSION = \"0.0.0\"\nend\n" > lib/dxruby/version.rb; \
	fi)
	(if [ -d "$(DNANEXUS_HOME)/.git" ]; then \
	    perl -e '$$ENV{GIT_TOOLKIT_VERSION} =~ /^v(\d+.\d+.\d+)/ and print "module DX\n  VERSION = \"$$1\"\nend\n"' > lib/dxruby/version.rb; \
	fi)

lib/dxruby/api.rb: ../../src/api_wrappers/wrapper_table.json generateRubyAPIWrappers.py
	cat ../../src/api_wrappers/wrapper_table.json | ./generateRubyAPIWrappers.py > ruby/lib/dxruby/api.rb
