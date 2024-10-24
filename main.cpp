#include <fstream>
#include <iostream>
#include <string>
#include <chrono>

void test_1_singlethread(const std::string& inputPath, const std::string& outputPath) {
    // Open the input file
    std::ifstream inputFile(inputPath);
    if (!inputFile.is_open()) {
        std::cerr << "Error opening input file." << std::endl;
        return;
    }

    // Open the output file
    std::ofstream outputFile(outputPath);
    if (!outputFile.is_open()) {
        std::cerr << "Error opening output file." << std::endl;
        return;
    }
    

    std::string line;
    size_t lineCount = 0;
    auto start = std::chrono::steady_clock::now();

    // Read from the input file and write to the output file
    while (std::getline(inputFile, line)) {
        outputFile << line << std::endl;
        ++lineCount;

        if (lineCount % 100000 == 0) {
            auto end = std::chrono::steady_clock::now();
            std::chrono::duration<double> elapsed = end - start;
            double seconds = elapsed.count();
            double rate = lineCount / seconds;
            std::cout << lineCount << " lines processed. Rate: " << std::fixed << std::setprecision(1) << rate / 1e6 << "m lines per second (LPS)." << std::endl;
        }
    }

    // Close both files
    inputFile.close();
    outputFile.close();

    std::cout << "Total lines processed: " << lineCount << std::endl;
    std::cout << "File has been successfully copied." << std::endl;
}


void processBuffer(const std::vector<char>& buffer, std::ofstream& outputFile, size_t& lineCount) {
    size_t start = 0;
    size_t end = buffer.size();
    
    // Process buffer to handle partial lines at the beginning and end
    for (size_t i = start; i < end; ++i) {
        if (buffer[i] == '\n') {
            // Write line from start to i (not including '\n')
            outputFile.write(&buffer[start], i - start);
            outputFile.put('\n');
            start = i + 1;  // move start to after '\n'
            ++lineCount;
        }
    }

    // Handle any remaining characters that did not end with a newline
    if (start < end) {
        outputFile.write(&buffer[start], end - start);
    }
}

void test_2_singlethread_buffered(const std::string& inputPath, const std::string& outputPath) {
    std::ifstream inputFile(inputPath, std::ios::binary);
    if (!inputFile.is_open()) {
        std::cerr << "Error opening input file." << std::endl;
        return;
    }

    std::ofstream outputFile(outputPath, std::ios::binary);
    if (!outputFile.is_open()) {
        std::cerr << "Error opening output file." << std::endl;
        return;
    }

    const size_t bufferSize = 1 << 22;  // 4MB buffer
    std::vector<char> buffer(bufferSize);
    size_t lineCount = 0;
    auto start = std::chrono::steady_clock::now();

    while (inputFile.read(buffer.data(), buffer.size()) || inputFile.gcount() > 0) {
        size_t bytes_read = inputFile.gcount();
        buffer.resize(bytes_read);  // Adjust buffer size to actual bytes read for processing
        processBuffer(buffer, outputFile, lineCount);

        if (lineCount % 100000 == 0) {
            auto end = std::chrono::steady_clock::now();
            std::chrono::duration<double> elapsed = end - start;
            double seconds = elapsed.count();
            double rate = lineCount / seconds;
            std::cout << lineCount << " lines processed. Rate: " << std::fixed << std::setprecision(1) << rate / 1e6 << "m lines per second (LPS)." << std::endl;
        }
    }

    outputFile.close();
    inputFile.close();

    std::cout << "Total lines processed: " << lineCount << std::endl;
    std::cout << "File has been successfully copied." << std::endl;
}

int main() {
    std::string inputPath = "/Users/steve/Downloads/fac_level_1m_pure.csv";
    std::string outputPath = "/Users/steve/Downloads/fac_level_1m_pure_out.csv";

    test_1_singlethread(inputPath, outputPath);

    test_2_singlethread_buffered(inputPath, outputPath);

    return 0;
}
