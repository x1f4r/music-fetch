# Third-party notices

Music Fetch includes the following third-party material.

## SwiftF0

The pitch model `core/crates/mfcore/src/melody/models/swiftf0.onnx` is the
`model.onnx` file of SwiftF0 0.3.0 (https://github.com/lars76/swift-f0),
unmodified. The pre- and post-processing in `core/crates/mfcore/src/melody/swiftf0.rs`
and the note segmentation in `core/crates/mfcore/src/melody/notes.rs` are ports
of the package's Python code.

```
MIT License

Copyright (c) 2025-2026 Lars Nieradzik

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
