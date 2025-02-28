

class CtaAdminServerWriteReactor : public CtaRpcStream::CallbackService

// request object will be filled in by the Parser of the command on the client-side.
// Currently I am calling this class CtaAdminCmdStreamingClient
grpc::ServerWriteReactor<StreamResponse>* GenericAdminStream(
    CallbackServerContext* context,
    const cta::xrd::Request* request) override {
  class Lister : public grpc::ServerWriteReactor<Feature> {
   public:
    Lister(const routeguide::Rectangle* rectangle,
           const std::vector<Feature>* feature_list)
        : left_((std::min)(rectangle->lo().longitude(),
                           rectangle->hi().longitude())),
          right_((std::max)(rectangle->lo().longitude(),
                            rectangle->hi().longitude())),
          top_((std::max)(rectangle->lo().latitude(),
                          rectangle->hi().latitude())),
          bottom_((std::min)(rectangle->lo().latitude(),
                             rectangle->hi().latitude())),
          feature_list_(feature_list),
          next_feature_(feature_list_->begin()) {
      NextWrite();
    }

    void OnWriteDone(bool ok) override {
      if (!ok) {
        Finish(Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure"));
      }
      NextWrite();
    }

    void OnDone() override {
      LOG(INFO) << "RPC Completed";
      delete this;
    }

    void OnCancel() override { LOG(ERROR) << "RPC Cancelled"; }

   private:
    void NextWrite() {
      while (next_feature_ != feature_list_->end()) {
        const Feature& f = *next_feature_;
        next_feature_++;
        if (f.location().longitude() >= left_ &&
            f.location().longitude() <= right_ &&
            f.location().latitude() >= bottom_ &&
            f.location().latitude() <= top_) {
          StartWrite(&f);
          return;
        }
      }
      // Didn't write anything, all is done.
      Finish(Status::OK);
    }
    const long left_;
    const long right_;
    const long top_;
    const long bottom_;
    const std::vector<Feature>* feature_list_;
    std::vector<Feature>::const_iterator next_feature_;
  };
  return new Lister(rectangle, &feature_list_);
}