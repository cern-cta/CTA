

class CtaRpcStreamImpl : public CtaRpcStream::CallbackService {
  public:
    cta::log::LogContext getLogContext() const { return m_lc; }
    CtaRpcStreamImpl();
    CtaRpcStreamImpl(cta::catalogue::Catalogue &catalogue) : m_catalogue(catalogue) {}
    /* CtaAdminServerWriteReactor is what the type of GenericAdminStream could be */
    grpc::ServerWriteReactor<cta::xrd::Response>* GenericAdminStream(CallbackServerContext* context, const cta::xrd::Request* request);

  private:
    cta::log::LogContext m_lc;
    cta::catalogue::Catalogue &m_catalogue;    //!< Reference to CTA Catalogue
    cta::Scheduler            &m_scheduler;    //!< Reference to CTA Scheduler
}

// serverWriteReactor<Feature> is the RETURN TYPE of the function which is server-side streaming!!!
// Is serverWriteReactor a class? yes it is
// but clientReadReactor is a class
class CtaAdminServerWriteReactor : public grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
  public:
    void OnWriteDone(bool ok) override {
      if (!ok) {
        Finish(Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
      }
      NextWrite();
    }
  private:
    // bool isHeaderSent = false;
    xrd::cta::Response stream_item_;
    std::vector<> // will hold the database query, each protobuf sent as the response is of the type cta::xrd::Response (which is either Header or Data)
    // for tapeLS for example, we hold the results in std::list<common::dataStructures::Tape> m_tapeList; - XrdCtaTapeLs.hpp implementation
}

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