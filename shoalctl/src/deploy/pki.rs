//! The cluster's certificate authority, and the leaf each node presents to its peers
//!
//! Every peer lane runs mutual TLS with the identity binding on
//! ([F50](../../../docs/src/features/cluster-operations.md)): a node's leaf has to name the node
//! as `shoal-node://<id>`, and both ends of every lane judge it. The deployment can issue that
//! leaf before the node's first start because the node program's `claim` prints the id the
//! directory was claimed with ([F51](../../../docs/src/features/cluster-deployment.md)).
//!
//! # Reloading the authority
//!
//! Only the authority's key is kept, and its certificate is distributed from the copy written at
//! mint. Signing a later leaf rebuilds an issuer from the key with the same distinguished name:
//! a verifier finds a leaf's issuer by that name and checks the signature against the key, so a
//! leaf signed by the rebuilt issuer chains to the distributed certificate. This keeps rcgen's
//! certificate parser out of the build.

use color_eyre::eyre::WrapErr;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use std::net::IpAddr;

/// A leaf certificate and its key, both as PEM
#[derive(Debug, Clone)]
pub struct Leaf {
    /// The certificate
    pub cert: String,
    /// Its private key
    pub key: String,
}

/// A cluster's certificate authority
pub struct Authority {
    /// The certificate to sign with
    issuer: Certificate,
    /// Its key
    key: KeyPair,
}

impl Authority {
    /// The parameters every issuer of a cluster is built from
    ///
    /// # Arguments
    ///
    /// * `cluster` - The cluster's name
    fn params(cluster: &str) -> color_eyre::Result<CertificateParams> {
        // an authority names no host
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        // the name a leaf's issuer is matched by, which has to be the same on every rebuild
        params
            .distinguished_name
            .push(DnType::CommonName, format!("shoal {cluster} authority"));
        Ok(params)
    }

    /// Mint a new authority for a cluster
    ///
    /// # Arguments
    ///
    /// * `cluster` - The cluster's name
    ///
    /// # Errors
    ///
    /// When a key or certificate cannot be generated.
    pub fn mint(cluster: &str) -> color_eyre::Result<Self> {
        // a fresh key, and a certificate for it signed by itself
        let key = KeyPair::generate()?;
        let issuer = Self::params(cluster)?.self_signed(&key)?;
        Ok(Authority { issuer, key })
    }

    /// Rebuild a cluster's authority from its key
    ///
    /// # Arguments
    ///
    /// * `cluster` - The cluster's name
    /// * `key_pem` - The authority's key as it was minted
    ///
    /// # Errors
    ///
    /// When the key does not parse.
    pub fn from_key(cluster: &str, key_pem: &str) -> color_eyre::Result<Self> {
        // the same key and the same name are the same issuer to a verifier
        let key = KeyPair::from_pem(key_pem).wrap_err("the authority key does not parse")?;
        let issuer = Self::params(cluster)?.self_signed(&key)?;
        Ok(Authority { issuer, key })
    }

    /// The authority's certificate, which every node trusts
    #[must_use]
    pub fn cert_pem(&self) -> String {
        self.issuer.pem()
    }

    /// The authority's key, which only the operator keeps
    #[must_use]
    pub fn key_pem(&self) -> String {
        self.key.serialize_pem()
    }

    /// Issue a node its leaf
    ///
    /// # Arguments
    ///
    /// * `node` - The node id its claim printed
    /// * `name` - The host name the operator calls it by
    /// * `address` - The address peers dial it at
    ///
    /// # Errors
    ///
    /// When a key or certificate cannot be generated.
    pub fn issue(&self, node: &str, name: &str, address: IpAddr) -> color_eyre::Result<Leaf> {
        // the leaf's own key
        let key = KeyPair::generate()?;
        // every name a peer might dial it by, and the node it is
        let mut params = CertificateParams::new(vec![name.to_string()])?;
        params.subject_alt_names.push(SanType::IpAddress(address));
        params.subject_alt_names.push(SanType::URI(
            format!("shoal-node://{node}")
                .try_into()
                .wrap_err("the node id does not make a uri")?,
        ));
        params
            .distinguished_name
            .push(DnType::CommonName, format!("{name} {node}"));
        // signed by the authority
        let cert = params.signed_by(&key, &self.issuer, &self.key)?;
        Ok(Leaf {
            cert: cert.pem(),
            key: key.serialize_pem(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shoal::shared::identity::NodeId;
    use shoal::shared::tls::{node_identity_of, peer_client_config, peer_server_config, PeerTlsOptions};

    /// A leaf names its node, and one signed by a rebuilt authority is accepted under the
    /// certificate distributed at mint
    #[test]
    fn a_leaf_names_its_node_under_a_rebuilt_authority() {
        // mint an authority and keep what a deployment keeps: the certificate and the key
        let minted = Authority::mint("lab").expect("an authority");
        let ca_pem = minted.cert_pem();
        let key_pem = minted.key_pem();
        drop(minted);
        // a later run rebuilds it from the key alone and issues a leaf
        let rebuilt = Authority::from_key("lab", &key_pem).expect("a rebuilt authority");
        let node = NodeId::mint();
        let leaf = rebuilt
            .issue(&node.to_string(), "hyperion", "172.16.2.5".parse().unwrap())
            .expect("a leaf");
        // the leaf names the node the way the binding reads it
        let der = rustls_pemfile::certs(&mut leaf.cert.as_bytes())
            .next()
            .expect("a certificate")
            .expect("a pem certificate");
        assert_eq!(node_identity_of(&der).expect("a readable san"), Some(node));
        // and the peer configs load it against the certificate written at mint
        let dir = std::env::temp_dir().join(format!("shoalctl-pki-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("node.pem"), &leaf.cert).unwrap();
        std::fs::write(dir.join("node.key"), &leaf.key).unwrap();
        std::fs::write(dir.join("ca.pem"), &ca_pem).unwrap();
        let options = PeerTlsOptions {
            cert: dir.join("node.pem"),
            key: dir.join("node.key"),
            ca: dir.join("ca.pem"),
            bind_identity: true,
        };
        peer_server_config(&options).expect("a server config");
        peer_client_config(&options).expect("a client config");
        std::fs::remove_dir_all(dir).ok();
    }
}
