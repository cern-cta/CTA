# Deployment Planning

Plan the CTA services, databases, tape hardware, and disk-system integration as separate parts of the deployment.

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Prerequisites and topology

Document supported deployment prerequisites, service placement, network paths, and resource sizing.

## Availability and failure domains

Document redundancy and dependencies for frontend, catalogue, scheduler, and tape services.

## Disk buffer requirements

Use the shared [Disk Buffer Concepts](../../concepts/components/disk-buffer.md) and the selected [integration guide](../integrations/index.md).

## CTA Services without Hardware Constraints

While `cta-taped` and `cta-rmcd` need to run on servers that have access to the tape hardware, the frontend/API services and `cta-maintd`
have no such constraints and can be run anywhere.

## Separate Frontends for Workflow Events and Admin Commands

The CTA frontend runs as two separate services:

 * the **Workflow Engine (WFE) Frontend**; and
 * the **Admin API**.

They both run the same software, only with different configuration files. This allows for greater flexibility in the software and
authentication architecture and prevents the load introduced by admin commands from interfering with the processing of workflow events.

It is advised to run at least 2 Admin API Frontend endpoints:

- one as an endpoint for **operators, operations monitoring systems and tape servers** with configured `cta-admin` CLI;
- another one that serves as **test/debug process**: used to debug crashing admin commands, deploy and test the next version of the Admin
API Frontend with no impact on production operations or monitoring.

## Scheduler isolation

Consider whether repack needs separate resources from archive and retrieve traffic. See [Scheduler Configuration](../configuration/scheduler.md#isolate-repack-with-separate-scheduler-backends).
