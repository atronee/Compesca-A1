test_mock:	src/mock.cpp
	g++ src/mock.cpp -o test_mock.exe -l sqlite3
	./test_mock.exe

TestDataframe: TestDataframe.cpp Dataframe.cpp Semaphore.cpp ConsumerProducer.h
	g++ TestDataframe.cpp Dataframe.cpp Semaphore.cpp -o TestDataframe.exe
	./TestDataframe.exe


TestReader: test_reader.cpp src/DataFrame.h src/DataFrame.cpp src/Reader.h src/Reader.cpp src/Semaphore.cpp src/Semaphore.cpp src/ConsumerProducerQueue.h
	g++ test_reader.cpp src/DataFrame.cpp src/Reader.cpp src/Semaphore.cpp -o TestReader.exe
	./TestReader.exe

# add_executable(TestPipeline
  #        test_pipeline.cpp
  #        src/DataFrame.h
  #        src/DataFrame.cpp
  #        src/Reader.h
  #        src/Reader.cpp
  #        src/Semaphore.h
  #        src/Semaphore.cpp
  #        src/ConsumerProducerQueue.h
  #        src/Handlers.h
  #        src/Handlers.cpp)

TestPipeline: test_pipeline.cpp src/DataFrame.h src/DataFrame.cpp src/Reader.h src/Reader.cpp src/Semaphore.cpp src/Semaphore.cpp src/ConsumerProducerQueue.h src/Handlers.h src/Handlers.cpp
	g++ test_pipeline.cpp src/DataFrame.cpp src/Reader.cpp src/Semaphore.cpp src/Handlers.cpp -o TestPipeline.exe
	./TestPipeline.exe

Dashboard: dashboardCalc.cpp src/DataFrame.h src/DataFrame.cpp src/Reader.h src/Reader.cpp src/Semaphore.cpp src/Semaphore.cpp src/triggers.cpp src/ConsumerProducerQueue.h src/Handlers.h src/Handlers.cpp
	g++ dashboardCalc.cpp src/DataFrame.cpp src/Reader.cpp src/Semaphore.cpp src/triggers.cpp src/Handlers.cpp -o dashboardCalc.exe
	./dashboardCalc.exe