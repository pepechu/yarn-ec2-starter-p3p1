current_dir = $(shell pwd -P)
THRIFT_PREFIX=/usr/local/thrift
RAPIDJSON_PREFIX=$(current_dir)/../lib/rapidjson-1.1.0

TARGETS = MyPolicyServer PolicySimulator
HPPFILES = Job.h Policy.h Utility.h
SVCHPPFILES = tetrisched_constants.h tetrisched_types.h YARNTetrischedService.h TetrischedService.h $(HPPFILES)
OBJS = tetrisched_constants.o tetrisched_types.o YARNTetrischedService.o TetrischedService.o

CXXFLAGS = -I$(RAPIDJSON_PREFIX)/include -g -O2 -std=c++11 -Wall
SVCCXXFLAGS = -I$(THRIFT_PREFIX)/include $(CXXFLAGS)
LDFLAGS = -L$(THRIFT_PREFIX)/lib -lthrift
CXX = g++

default:	$(TARGETS)
all:		$(TARGETS)

MyPolicyServer: $(OBJS) MyPolicyServer.o
	$(CXX) $(SVCCXXFLAGS) -o $@ $^ $(LDFLAGS)

PolicySimulator: PolicySimulator.o
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: %.cpp $(HPPFILES)
	$(CXX) $(SVCCXXFLAGS) -c -o $@ $<

MyPolicyServer.o: PolicyServerContainer.cc PolicyServerContainer.h PolicyServerIf.h \
		PhaseOnePolicyServer.h PhaseOnePolicyServerHandler.h YARNTetrischedService.h SchedulerTransport.h
	$(CXX) $(SVCCXXFLAGS) -c -o $@ $<

PolicySimulator.o: PolicySimulator.cc PhaseOnePolicyServerHandler.h $(HPPFILES)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	-rm -f $(TARGETS) *.o
