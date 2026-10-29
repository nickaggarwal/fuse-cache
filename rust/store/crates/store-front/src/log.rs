//! Log lines in the Go client's style: `[FRONT] 2026/10/28 15:04:05 msg`
//! (Go `log.New(w, "[FRONT] ", log.LstdFlags)`), local time, to stderr.

pub fn stamp() -> String {
    unsafe {
        let mut t: libc::time_t = 0;
        libc::time(&mut t);
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&t, &mut tm);
        format!("{:04}/{:02}/{:02} {:02}:{:02}:{:02}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
    }
}

#[macro_export]
macro_rules! flog {
    ($($arg:tt)*) => {
        eprintln!("[FRONT] {} {}", $crate::log::stamp(), format!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn stamp_shape() {
        let s = super::stamp();
        assert_eq!(s.len(), 19);
        assert_eq!(&s[4..5], "/");
        assert_eq!(&s[13..14], ":");
    }
}
