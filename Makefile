test_mock:	src/mock.cpp
	g++ src/mock.cpp -o test_mock.exe
	./test_mock.exe

TestDataframe: TestDataframe.cpp Dataframe.cpp Semaphore.cpp ConsumerProducer.h
	g++ TestDataframe.cpp Dataframe.cpp Semaphore.cpp -o TestDataframe.exe
	./TestDataframe.exe


TestReader: test_reader.cpp src/DataFrame.h src/DataFrame.cpp src/Reader.h src/Reader.cpp src/Semaphore.cpp src/Semaphore.cpp src/ConsumerProducerQueue.h
	g++ test_reader.cpp src/DataFrame.cpp src/Reader.cpp src/Semaphore.cpp -o TestReader.exe
	./TestReader.exe