;;;; A plugin in Lisp, on the SDK of rontolisp's examples/proxy-wasm (build.sh fetches it).
;;;;
;;;; It stamps responses, answers /hello itself, and keeps what /hold allocates.

(require :proxy-wasm "proxy-wasm.lisp")

(defpackage :lisp-plugin (:use :cl))

(in-package :lisp-plugin)

(defvar *held* nil)

(defmethod proxy-wasm:on-request-headers
    ((stream proxy-wasm:http-context) num-headers end-of-stream)
  (declare (ignore num-headers end-of-stream))
  (cond ((equal (proxy-wasm:get-header :request ":path") "/hello")
         (proxy-wasm:send-local-response 200 :body (format nil "Hello from Lisp~%"))
         :pause)
        (t :continue)))

(defmethod proxy-wasm:on-response-headers
    ((stream proxy-wasm:http-context) num-headers end-of-stream)
  (declare (ignore num-headers end-of-stream))
  (when (equal (proxy-wasm:get-header :request ":path") "/hold")
    (push (make-array 1048576 :element-type '(unsigned-byte 8)) *held*))
  (proxy-wasm:add-header :response "x-filtered" "yes")
  :continue)
