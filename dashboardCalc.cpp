#include "src/DataFrame.h"
#include "src/ConsumerProducerQueue.h"
#include "src/Handlers.h"
#include "src/Reader.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <mutex>
#include <thread>
#include <atomic>
#include <iostream>
#include "src/triggers.h"

int main() {
    // answer the questions save the results of a "log"/ dashboard file
    // 1º question: Número de produtos visualizados por minuto

    // let's use the logs named userBehavior_1..10.csv

    // std::vector<const std::type_info *> typesUserBehavior = {&typeid(std::string), &typeid(int), &typeid(std::string),
    //                                                          &typeid(int), &typeid(std::string), &typeid(std::string),
    //                                                          &typeid(std::string), &typeid(std::string)};

    // // create a reader object
    // FileReader csvReader;
    // // creates the queues
    // ConsumerProducerQueue<std::string> queue_files(15);
    // ConsumerProducerQueue<DataFrame *> queue_reader(15);
    // ConsumerProducerQueue<DataFrame *> queue_select(15);
    // ConsumerProducerQueue<DataFrame *> queue_filter(15);

    // EventBasedTrigger eventTrigger;
    // std::thread eventTriggerThread([&eventTrigger, &queue_files] {
    //     eventTrigger.triggerOnApperanceOfNewLogFile("./logs", queue_files);
    // });

    // //void FileReader::read(const std::string& filename, std::vector<const std::type_info*>& types, char delimiter,
    // //                      int start, int & end, ConsumerProducerQueue<DataFrame*>& queue,
    // //                      bool read_in_blocks, int block_size)

    // auto selector = SelectHandler(&queue_reader, &queue_select);
    // auto filter = FilterHandler(&queue_select, &queue_filter);
    // auto printer = printHandler(&queue_filter);

    std::vector<std::thread> threads;

    // int end = 0;
    // threads.emplace_back([&csvReader, &typesUserBehavior, &end, &queue_reader,  &queue_files] {
    //     csvReader.read(typesUserBehavior, ',', 0, end, std::ref(queue_reader), std::ref(queue_files), true, 10, "user_behavior");
    // });

    // std::cout<<"Selecting\n"<<std::endl;

    // for (int i = 0; i < 2; i++) {
    //     threads.emplace_back([&selector] {
    //         selector.select({"User Author Id","Button Product Id", "Date"});
    //     });
    // }
    // std::cout<<"Filtering\n"<<std::endl;

    // for (int i = 0; i < 2; i++) {
    //     threads.emplace_back([&filter] {
    //         filter.filter("Button Product Id", "!=", "0");
    //     });
    // }

    // std::cout<<"Printing\n"<<std::endl;

    // threads.emplace_back([&printer] {
    //     printer.print();
    // });

    // for (auto &t: threads) {
    //     t.join();
    // }


    // 6º question: Número de produtos vendidos sem disponibilidade em estoque

    DataFrame* df_ptr = new DataFrame();

    vector<int> id_data = {1, 2, 3, 4,5,6,7};
    vector<int> estoque_data = {528,219,408,574,568,224,464};


    df_ptr->add_column("id", id_data);
    df_ptr->add_column("estoque", estoque_data);
    FileReader csvReader6;

    ConsumerProducerQueue<std::string> queue_files6(15);
    ConsumerProducerQueue<DataFrame *> queue_reader6(15);
    ConsumerProducerQueue<DataFrame *> queue_select6(15);
    ConsumerProducerQueue<DataFrame *> queue_filter6(15);
    ConsumerProducerQueue<DataFrame *> queue_gb6(15);
    ConsumerProducerQueue<DataFrame *> queue_join6(15);
    ConsumerProducerQueue<DataFrame *> queue_print6(15);
    ConsumerProducerQueue<DataFrame *> queue_agregator(15);

    EventBasedTrigger eventTrigger6;
    std::thread eventTriggerThread6([&eventTrigger6, &queue_files6] {
        eventTrigger6.triggerOnApperanceOfNewLogFile("./data", queue_files6);
    });

    // wait 5 seconds for the file to be created
    std::this_thread::sleep_for(std::chrono::seconds(10));
    queue_files6.push("STOP");

    std::vector<const std::type_info *> order_types = {&typeid(int), &typeid(int),
                                                       &typeid(int), 
                                                       &typeid(std::string), &typeid(std::string), 
                                                       &typeid(std::string), &typeid(std::string)};
    
    int end6 = 0;
    threads.emplace_back([&csvReader6, &order_types, &end6, &queue_reader6,  &queue_files6] {
        csvReader6.read(order_types, ',', 0, end6, std::ref(queue_reader6), std::ref(queue_files6), true, 10, "order");
    });

    auto selector6 = SelectHandler(&queue_reader6, &queue_select6);

    for (int i = 0; i < 2; i++) {
        threads.emplace_back([&selector6] {
            selector6.select({"ID PRODUTO", "QUANTIDADE"});
        });
    }


    auto groupby = GroupByHandler(&queue_select6, &queue_gb6);
    threads.emplace_back([&groupby] {
        groupby.group_by("ID PRODUTO", "sum");
    });

    auto joiner6 = JoinHandler(&queue_gb6, &queue_join6);
    threads.emplace_back([df_ptr, &joiner6] {
        joiner6.join(df_ptr, "id", "ID PRODUTO");
    });

    // auto agregator = FinalHandler(&queue_join6, &queue_agregator);
    // threads.emplace_back([&agregator] {
    //     agregator.aggregate();
    // });

    //pop from queue_agregator
    DataFrame* df = queue_join6.pop();
    df->diff_columns("estoque", "QUANTIDADE");
    
    queue_filter6.push(df);

    auto filter6 = FilterHandler(&queue_filter6, &queue_print6);

    for (int i = 0; i < 2; i++) {
        threads.emplace_back([&filter6] {
            filter6.filter("diff", "<", "0");
        });
    }

    df=queue_print6.pop();
    int sum = df->sum_column("diff");
    sum = sum * -1;
    std::cout<<"Número de produtos vendidos sem disponibilidade em estoque: "<<sum<<std::endl;
    

    for (auto &t: threads) {
        t.join();
    }

    return 0;
}



