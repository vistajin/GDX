/*
 * COPYRIGHT. HSBC HOLDINGS PLC 2015. ALL RIGHTS RESERVED.
 *
 * This software is only to be used for the purpose for which it has been
 * provided. No part of it is to be reproduced, disassembled, transmitted,
 * stored in a retrieval system nor translated in any human or computer
 * language in any way or for any other purposes whatsoever without the
 * prior written consent of HSBC Holdings plc.
 */
package com.hsbc.gdx.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.hsbc.gdx.jpa.domain.ActivityLog;
import com.hsbc.gdx.jpa.domain.Recipient;
import com.hsbc.gdx.scheduler.support.DaoManager;
import com.hsbc.gdx.scheduler.util.ActivityLogGWBIDataBean;
import com.hsbc.gdx.scheduler.util.GWBIFileWriter;
import com.hsbc.gdx.scheduler.util.JobConstants;

/**
 * This is the executer that trigger the sending of email to recipient
 */
public class BusinessInfoJob extends QuartzJobBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String dataFilePath;

    @Autowired
    private DaoManager daoManager;

    @Override
    protected void executeInternal(final JobExecutionContext context)
        throws JobExecutionException {

        @SuppressWarnings("rawtypes")
        Map jobDataMap = context.getMergedJobDataMap();

        this.dataFilePath = (String)jobDataMap.get(JobConstants.GWBI_LOG_FILE_PATH_KEY);

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Runing BusinessInfoJob.executeInternal().....");
            this.logger.debug("Data export file path is " + this.dataFilePath);
        }

        List<ActivityLogGWBIDataBean> logExportBeans = new ArrayList<ActivityLogGWBIDataBean>();

        try {
            List<ActivityLog> activityLogs = this.daoManager.getActivitiesForBI();

            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Retrieved Activity Logs = " + activityLogs);
            }

            for (ActivityLog activityLog: activityLogs) {
                switch (activityLog.getAction()) {
                case REQUEST:
                    logExportBeans.add(populateRequestData(activityLog));
                    break;
                case LINK:
                case REJECT:
                    logExportBeans.add(populateLinkRejectData(activityLog));
                    break;
                case EXPIRE:
                    logExportBeans.add(populateExipryData(activityLog));
                    break;
                default:
                    logExportBeans.add(populateOtherData(activityLog));
                    break;
                }
            }

            if (this.logger.isDebugEnabled()) {
                this.logger.debug("The list of populated activity data bean is " + logExportBeans);
            }

            GWBIFileWriter.writeToFile(logExportBeans, this.dataFilePath);

        } catch (Exception e) {
            this.logger.error("Exception when generating activity logs file: " + e.getMessage());
        }
    }

    private ActivityLogGWBIDataBean populateRequestData(final ActivityLog log) {

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("In populateRequestData for action type " + log.getAction());
        }

        ActivityLogGWBIDataBean dataBean = new ActivityLogGWBIDataBean();
        Recipient recipient = log.getRecipient();

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Retrieved Recipient is " + recipient);
        }

        dataBean.setAction(log.getAction());
        dataBean.setSenderEntity(recipient.getSenderCountryCode() + recipient.getSenderGroupMember());
        dataBean.setSenderID(log.getSenderID());
        dataBean.setRecipientID(log.getRecipient().getId());
        dataBean.setSenderName(recipient.getSenderName());
        dataBean.setReceiverNickname(recipient.getNickname());
        dataBean.setActivityTime(log.getActivityTime());
        dataBean.setSenderCustomerNumber(recipient.getSenderCustomerNumber());

        return dataBean;
    }

    private ActivityLogGWBIDataBean populateLinkRejectData(final ActivityLog log) {

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("In populateLinkRejectData for action type " + log.getAction());
        }

        ActivityLogGWBIDataBean dataBean = new ActivityLogGWBIDataBean();
        Recipient recipient = log.getRecipient();

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Retrieved Recipient is " + recipient);
        }

        dataBean.setAction(log.getAction());
        dataBean.setSenderEntity(recipient.getSenderCountryCode() + recipient.getSenderGroupMember());
        dataBean.setRecipientID(log.getRecipient().getId());
        dataBean.setActivityTime(log.getActivityTime());
        dataBean.setRecipientEntity(recipient.getRecipientCountryCode() + recipient.getRecipientGroupMember());
        dataBean.setCustomerNumber(recipient.getCustomerNumber());
        dataBean.setSenderCustomerNumber(recipient.getSenderCustomerNumber());

        return dataBean;
    }

    private ActivityLogGWBIDataBean populateExipryData(final ActivityLog log) {

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("In populateExpiryData for action type " + log.getAction());
        }

        ActivityLogGWBIDataBean dataBean = new ActivityLogGWBIDataBean();
        Recipient recipient = log.getRecipient();

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Retrieved Recipient is " + recipient);
        }

        dataBean.setAction(log.getAction());
        dataBean.setSenderEntity(recipient.getSenderCountryCode() + recipient.getSenderGroupMember());
        dataBean.setRecipientID(log.getRecipient().getId());
        dataBean.setExpireDate(recipient.getInvitation().getExpiryDate());
        dataBean.setActivityTime(log.getActivityTime());
        dataBean.setSenderCustomerNumber(recipient.getSenderCustomerNumber());

        return dataBean;
    }

    private ActivityLogGWBIDataBean populateOtherData (final ActivityLog log) {
        ActivityLogGWBIDataBean dataBean = new ActivityLogGWBIDataBean();

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("In populateOtherData for action type " + log.getAction());
        }

        Recipient recipient = log.getRecipient();

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Retrieved Recipient is " + recipient);
        }

        dataBean.setAction(log.getAction());
        dataBean.setSenderEntity(recipient.getSenderCountryCode() + recipient.getSenderGroupMember());
        dataBean.setRecipientID(log.getRecipient().getId());
        dataBean.setActivityTime(log.getActivityTime());
        dataBean.setSenderCustomerNumber(recipient.getSenderCustomerNumber());

        return dataBean;
    }
}
