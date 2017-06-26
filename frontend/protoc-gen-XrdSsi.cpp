#include <iostream> // just for debug output
#include <memory>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/zero_copy_stream.h>

// Strip .proto from filenames

void strip_proto(std::string &filename)
{
   using namespace std;

   auto truncate = filename.find(".proto", filename.length()-6);
     
   if(truncate != string::npos)
   {
      filename.resize(truncate);
   }
}

// Google protocol buffer code generator for XRootD SSI service

class XrdSsiGenerator : public google::protobuf::compiler::CodeGenerator
{
public:
   XrdSsiGenerator() {}
   virtual ~XrdSsiGenerator() {}

   virtual bool Generate(const google::protobuf::FileDescriptor *file,
                         const std::string &parameter,
                         google::protobuf::compiler::GeneratorContext *generator_context,
                         std::string *error) const;
#if 0
 private:
  // Insert the given code into the given file at the given insertion point.
  void Insert(grpc::protobuf::compiler::GeneratorContext *context,
              const grpc::string &filename, const grpc::string &insertion_point,
              const grpc::string &code) const {
    std::unique_ptr<grpc::protobuf::io::ZeroCopyOutputStream> output(
        context->OpenForInsert(filename, insertion_point));
    grpc::protobuf::io::CodedOutputStream coded_out(output.get());
    coded_out.WriteRaw(code.data(), code.size());
  }
#endif
};

bool XrdSsiGenerator::Generate(const google::protobuf::FileDescriptor *file,
                               const std::string &parameter,
                               google::protobuf::compiler::GeneratorContext *generator_context,
                               std::string *error) const
{
   using namespace std;

   // Ensure that generic services is disabled in the .proto file

   if (file->options().cc_generic_services())
   {
      *error = "XrdSsi proto compiler plugin does not work with generic services: set cc_generic_services = false.";

      return false;
   }

   // Create the output files

   string filename_base = file->name();
   strip_proto(filename_base);

   unique_ptr<google::protobuf::io::ZeroCopyOutputStream> header_output(generator_context->Open(filename_base + ".xrdssi.pb.h"));

   void *data;
   int bufsize;

   header_output->Next(&data, &bufsize);

   strcpy(reinterpret_cast<char*>(data), "Hello, world!");

   return true;
}

#if 0
{

    grpc_cpp_generator::Parameters generator_parameters;
    generator_parameters.use_system_headers = true;
    generator_parameters.generate_mock_code = false;

    ProtoBufFile pbfile(file);

    if (!parameter.empty()) {
      std::vector<grpc::string> parameters_list =
          grpc_generator::tokenize(parameter, ",");
      for (auto parameter_string = parameters_list.begin();
           parameter_string != parameters_list.end(); parameter_string++) {
        std::vector<grpc::string> param =
            grpc_generator::tokenize(*parameter_string, "=");
        if (param[0] == "services_namespace") {
          generator_parameters.services_namespace = param[1];
        } else if (param[0] == "use_system_headers") {
          if (param[1] == "true") {
            generator_parameters.use_system_headers = true;
          } else if (param[1] == "false") {
            generator_parameters.use_system_headers = false;
          } else {
            *error = grpc::string("Invalid parameter: ") + *parameter_string;
            return false;
          }
        } else if (param[0] == "grpc_search_path") {
          generator_parameters.grpc_search_path = param[1];
        } else if (param[0] == "generate_mock_code") {
          if (param[1] == "true") {
            generator_parameters.generate_mock_code = true;
          } else if (param[1] != "false") {
            *error = grpc::string("Invalid parameter: ") + *parameter_string;
            return false;
          }
        } else {
          *error = grpc::string("Unknown parameter: ") + *parameter_string;
          return false;
        }
      }
    }

    grpc::string file_name = grpc_generator::StripProto(file->name());

    grpc::string header_code =
        grpc_cpp_generator::GetHeaderPrologue(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetHeaderIncludes(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetHeaderServices(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetHeaderEpilogue(&pbfile, generator_parameters);
    std::unique_ptr<grpc::protobuf::io::ZeroCopyOutputStream> header_output(
        context->Open(file_name + ".grpc.pb.h"));
    grpc::protobuf::io::CodedOutputStream header_coded_out(header_output.get());
    header_coded_out.WriteRaw(header_code.data(), header_code.size());

    grpc::string source_code =
        grpc_cpp_generator::GetSourcePrologue(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetSourceIncludes(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetSourceServices(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetSourceEpilogue(&pbfile, generator_parameters);
    std::unique_ptr<grpc::protobuf::io::ZeroCopyOutputStream> source_output(
        context->Open(file_name + ".grpc.pb.cc"));
    grpc::protobuf::io::CodedOutputStream source_coded_out(source_output.get());
    source_coded_out.WriteRaw(source_code.data(), source_code.size());

    if (!generator_parameters.generate_mock_code) {
      return true;
    }
    grpc::string mock_code =
        grpc_cpp_generator::GetMockPrologue(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetMockIncludes(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetMockServices(&pbfile, generator_parameters) +
        grpc_cpp_generator::GetMockEpilogue(&pbfile, generator_parameters);
    std::unique_ptr<grpc::protobuf::io::ZeroCopyOutputStream> mock_output(
        context->Open(file_name + "_mock.grpc.pb.h"));
    grpc::protobuf::io::CodedOutputStream mock_coded_out(mock_output.get());
    mock_coded_out.WriteRaw(mock_code.data(), mock_code.size());

    return true;
  }
#endif

int main(int argc, char* argv[])
{
   XrdSsiGenerator generator;

   return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
