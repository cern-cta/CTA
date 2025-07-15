#pragma once

#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "CtaAdminServerWriteReactor.hpp"
#include <tuple>

namespace cta::frontend::grpc {
template<typename T, typename MessageItem, typename FillFunc, typename... ExtraArgs>
class TemplateAdminCmdStream : public CtaAdminServerWriteReactor {
public:
    using Iterator = typename std::list<T>::const_iterator;

    TemplateAdminCmdStream(cta::catalogue::Catalogue &catalogue,
                               cta::Scheduler &scheduler,
                               const std::string& instanceName,
                               const std::list<T>& itemList,
                               cta::admin::HeaderType headerType,
                               FillFunc fillFunc,
                               ExtraArgs... extraArgs)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
      m_itemList(itemList), m_fillFunc(fillFunc), m_headerType(headerType), m_extraArgs(extraArgs...)
    {
        m_iter = m_itemList.cbegin();
        NextWrite();
    }

    void NextWrite() override {
        std::cout << "Inside Template::NextWrite()!" << std::endl;
        m_response.Clear();
        if (!m_isHeaderSent) {
            std::cout << "About to send the header" << std::endl;
            auto *header = new cta::xrd::Response();
            header->set_type(cta::xrd::Response::RSP_SUCCESS);
            header->set_show_header(m_headerType);
            m_response.set_allocated_header(header);
            m_isHeaderSent = true;
            CtaAdminServerWriteReactor::StartWrite(&m_response);
            return;
        }
        std::cout << "Inside Template::NextWrite(), header was sent, about to start with the data" << std::endl;
        while (m_iter != m_itemList.cend()) {
            const auto& item = *m_iter++;
            auto *data = new cta::xrd::Data();
            MessageItem *messageItem = getMessageField(data);
            // Properly expand the tuple
            std::apply([&](const ExtraArgs&... args) {
                m_fillFunc(item, messageItem, m_instanceName, args...);
            }, m_extraArgs);
            m_response.set_allocated_data(data);
            CtaAdminServerWriteReactor::StartWrite(&m_response);
            return;
        }

        CtaAdminServerWriteReactor::Finish(::grpc::Status::OK);
    }

protected:
    virtual MessageItem* getMessageField(cta::xrd::Data* data) = 0;

    std::list<T> m_itemList;
    Iterator m_iter;
    FillFunc m_fillFunc;
    cta::admin::HeaderType m_headerType;
    std::tuple<ExtraArgs...> m_extraArgs;
};
}
