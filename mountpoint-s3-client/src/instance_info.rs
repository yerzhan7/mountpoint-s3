//! A simple interface to retrieve information about the EC2 instance this client is running on.
//!
//! The instance type is read from the DMI (Desktop Management Interface) data, the hardware
//! identification that the Linux kernel exposes from the system firmware, when available. Otherwise,
//! as for the region, it is retrieved by querying the Instance Metadata Service (IMDS).

use std::cell::LazyCell;
use std::env;
use std::fs;
use std::path::Path;

use thiserror::Error;

use crate::imds_crt_client::{IdentityDocument, ImdsCrtClient, ImdsQueryRequestError};

/// Directory where the Linux kernel exposes the DMI data of the machine.
const DMI_ID_DIR: &str = "/sys/devices/virtual/dmi/id";

/// Information on the EC2 instance. The instance type is read from DMI if available, otherwise it
/// is retrieved from the IMDS client, as is the region. Both sources are queried lazily, and the
/// IMDS client only if the `AWS_EC2_METADATA_DISABLED` environment variable is not set.
#[derive(Debug)]
pub struct InstanceInfo {
    dmi_instance_type: LazyCell<Option<String>>,
    document: LazyCell<Result<IdentityDocument, InstanceInfoError>>,
}

impl InstanceInfo {
    /// Create a new instance. DMI and the IMDS client will only be queried when a method on the
    /// instance is called, and the IMDS client only if the `AWS_EC2_METADATA_DISABLED` environment
    /// variable is not set.
    pub fn new() -> Self {
        Self {
            dmi_instance_type: LazyCell::new(|| read_dmi_instance_type(Path::new(DMI_ID_DIR))),
            document: LazyCell::new(|| {
                if !imds_disabled() {
                    match retrieve_instance_identity_document() {
                        Ok(identity_document) => {
                            tracing::debug!(?identity_document, "got instance info from IMDS");
                            Ok(identity_document)
                        }
                        Err(err) => {
                            tracing::debug!("EC2 instance info not retrieved: {err:?}");
                            Err(err)
                        }
                    }
                } else {
                    tracing::debug!("EC2 instance info not retrieved: IMDS was disabled");
                    Err(InstanceInfoError::ImdsDisabled)
                }
            }),
        }
    }

    /// The region for the current instance, if it can be retrieved using the IMDS client.
    pub fn region(&self) -> Result<&str, &InstanceInfoError> {
        self.document.as_ref().map(|d| d.region.as_str())
    }

    /// The instance type for the current instance, if it can be read from DMI or retrieved using
    /// the IMDS client. IMDS is not queried if DMI provides the instance type.
    pub fn instance_type(&self) -> Result<&str, &InstanceInfoError> {
        match self.dmi_instance_type.as_deref() {
            Some(instance_type) => Ok(instance_type),
            None => self.document.as_ref().map(|d| d.instance_type.as_str()),
        }
    }
}

impl Default for InstanceInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Read the instance type from the DMI data in `dmi_dir`. On EC2, the system vendor is "Amazon EC2"
/// and the product name is the instance type. This mirrors the CRT's detection in
/// `aws_s3_get_ec2_instance_type`, and needs neither network access nor elevated privileges since
/// both files are world-readable.
fn read_dmi_instance_type(dmi_dir: &Path) -> Option<String> {
    let read = |file_name: &str| {
        let path = dmi_dir.join(file_name);
        fs::read_to_string(&path)
            .inspect_err(|err| tracing::debug!(?path, "EC2 instance type not read from DMI: {err}"))
            .ok()
    };

    let sys_vendor = read("sys_vendor")?;
    let sys_vendor = sys_vendor.trim();
    if !sys_vendor.eq_ignore_ascii_case("Amazon EC2") {
        tracing::debug!(
            sys_vendor,
            "EC2 instance type not read from DMI: system vendor is not Amazon EC2"
        );
        return None;
    }

    let product_name = read("product_name")?;
    let instance_type = product_name.trim();
    if instance_type.is_empty() {
        tracing::debug!("EC2 instance type not read from DMI: product name is empty");
        return None;
    }

    tracing::debug!(instance_type, "got instance type from DMI");
    Some(instance_type.to_owned())
}

fn retrieve_instance_identity_document() -> Result<IdentityDocument, InstanceInfoError> {
    let imds_crt_client = ImdsCrtClient::new().map_err(InstanceInfoError::ImdsClientFailed)?;

    let identity_document = futures::executor::block_on(imds_crt_client.get_identity_document())?;
    Ok(identity_document)
}

fn imds_disabled() -> bool {
    match env::var_os("AWS_EC2_METADATA_DISABLED") {
        Some(val) => !val.eq_ignore_ascii_case("false"),
        None => false,
    }
}

/// Errors returned by instance info queries
#[derive(Debug, Error)]
pub enum InstanceInfoError {
    /// IMDS is disabled
    #[error("IMDS is disabled")]
    ImdsDisabled,

    /// A query to IMDS failed
    #[error("IMDS query failed: {0}")]
    ImdsQueryFailed(#[from] ImdsQueryRequestError),

    /// The IMDS client couldn't be constructed
    #[error("could not construct IMDS client: {0}")]
    ImdsClientFailed(mountpoint_s3_crt::common::error::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case(Some("Amazon EC2\n"), Some("m5.4xlarge\n"), Some("m5.4xlarge"); "ec2 instance")]
    #[test_case(Some("amazon ec2"), Some("c7gn.16xlarge"), Some("c7gn.16xlarge"); "vendor is case insensitive")]
    #[test_case(Some("Xen\n"), Some("HVM domU\n"), None; "xen instance")]
    #[test_case(Some("QEMU\n"), Some("Standard PC (Q35 + ICH9, 2009)\n"), None; "not ec2")]
    #[test_case(Some("Amazon EC2\n"), Some(" \n"), None; "empty product name")]
    #[test_case(Some("Amazon EC2\n"), None, None; "missing product name")]
    #[test_case(None, Some("m5.4xlarge\n"), None; "missing system vendor")]
    fn test_read_dmi_instance_type(sys_vendor: Option<&str>, product_name: Option<&str>, expected: Option<&str>) {
        let dmi_dir = tempfile::tempdir().unwrap();
        if let Some(sys_vendor) = sys_vendor {
            fs::write(dmi_dir.path().join("sys_vendor"), sys_vendor).unwrap();
        }
        if let Some(product_name) = product_name {
            fs::write(dmi_dir.path().join("product_name"), product_name).unwrap();
        }

        assert_eq!(read_dmi_instance_type(dmi_dir.path()).as_deref(), expected);
    }
}
