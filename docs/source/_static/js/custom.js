/*
    # Copyright 2026 Goldman Sachs
    #
    # Licensed under the Apache License, Version 2.0 (the "License");
    # you may not use this file except in compliance with the License.
    # You may obtain a copy of the License at
    #
    #      http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS,
    # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    # See the License for the specific language governing permissions and
    # limitations under the License.
 */

// 1. Dark Mode Toggle
(function () {
    var savedTheme = localStorage.getItem('pylegend-theme');
    var toggleBtn = document.getElementById('theme-toggle-btn');
    var isDarkMode = savedTheme === 'dark' || !savedTheme;

    if (isDarkMode) {
        document.body.classList.add('dark-mode');
        document.documentElement.classList.add('dark-mode');
        if (toggleBtn) toggleBtn.classList.add('active');
    }

    if (toggleBtn) {
        toggleBtn.addEventListener('click', function () {
            document.body.classList.toggle('dark-mode');
            document.documentElement.classList.toggle('dark-mode');
            this.classList.toggle('active');
            localStorage.setItem('pylegend-theme', document.body.classList.contains('dark-mode') ? 'dark' : 'light');
        });
    }
})();

// 2. Sidebar Link Activation & Scroll Progress
document.addEventListener('DOMContentLoaded', function () {
    var sections = document.querySelectorAll('section[id]');
    var sidebarLinks = document.querySelectorAll('.sphinxsidebar a.reference.internal');
    var bar = document.getElementById('reading-progress-bar');

    // Scroll Progress
    window.addEventListener('scroll', function () {
        if (!bar) return;
        var scrollTop = window.scrollY;
        var docHeight = document.documentElement.scrollHeight - window.innerHeight;
        var progress = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;
        bar.style.width = progress + '%';
    });

    if (!sections.length || !sidebarLinks.length) return;

    function activate(id) {
        sidebarLinks.forEach(function (link) {
            link.classList.remove('current');
        });
        sidebarLinks.forEach(function (link) {
            var href = link.getAttribute('href');
            if (href === '#' + id || href.endsWith('#' + id)) {
                link.classList.add('current');
                var parent = link.closest('li');
                while (parent) {
                    var ul = parent.parentElement;
                    if (ul && ul.tagName === 'UL') {
                        var parentLi = ul.closest('li');
                        if (parentLi) {
                            var parentLink = parentLi.querySelector(':scope > a');
                            if (parentLink) parentLink.classList.add('current');
                        }
                    }
                    parent = ul ? ul.closest('li') : null;
                }
            }
        });
    }

    var observer = new IntersectionObserver(function (entries) {
        entries.forEach(function (entry) {
            if (entry.isIntersecting) activate(entry.target.id);
        });
    }, { rootMargin: '0px 0px -70% 0px', threshold: 0 });

    sections.forEach(function (section) { observer.observe(section); });
});

// 3. Line-level Copy Buttons
document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('.highlight').forEach(function (block) {
        if (block.querySelector('.copy-line-btn')) return;

        var prompts = Array.from(block.querySelectorAll('.gp')).filter(function (gp) {
            return gp.innerText.match(/^(In \[\d+\]|>>>)/);
        });

        if (prompts.length === 0) return;

        var text = block.querySelector('pre').innerText;
        var cmdRegex = /(?:^|\n)(In \[\d+\]:|>>>) /g;
        var match;
        var indices = [];
        while ((match = cmdRegex.exec(text)) !== null) {
            indices.push({ start: match.index, promptLen: match[0].length, prompt: match[1] });
        }

        prompts.forEach(function (gp, i) {
            if (i >= indices.length) return;
            var btn = document.createElement('button');
            btn.className = 'copy-line-btn';
            btn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" style="width:16px;height:16px"><path stroke-linecap="round" stroke-linejoin="round" d="M16.5 8.25V6a2.25 2.25 0 00-2.25-2.25H6A2.25 2.25 0 003.75 6v8.25A2.25 2.25 0 006 16.5h2.25m8.25-8.25H11.812a2.25 2.25 0 00-2.25 2.25v8.25a2.25 2.25 0 002.25 2.25h6.188a2.25 2.25 0 002.25-2.25V8.25a2.25 2.25 0 00-2.25-2.25z" /></svg>';
            btn.title = "Copy command";
            var pre = block.querySelector('pre');
            pre.style.position = 'relative';
            btn.style.top = gp.offsetTop + 'px';
            btn.style.right = '0px';
            pre.appendChild(btn);

            btn.addEventListener('click', function () {
                var startIdx = indices[i].start + indices[i].promptLen;
                var endIdx = (i + 1 < indices.length) ? indices[i + 1].start : text.length;
                var rawCode = text.substring(startIdx, endIdx);
                var outMatch = rawCode.match(/\nOut\[\d+\]:/);
                if (outMatch) rawCode = rawCode.substring(0, outMatch.index);
                var code = rawCode.replace(/^\s*\.\.\.: /gm, '').replace(/^\s*\.\.\. /gm, '').trim();

                navigator.clipboard.writeText(code).then(function () {
                    btn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" style="width:16px;height:16px"><path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" /></svg>';
                    btn.classList.add('success');
                    setTimeout(function () {
                        btn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" style="width:16px;height:16px"><path stroke-linecap="round" stroke-linejoin="round" d="M16.5 8.25V6a2.25 2.25 0 00-2.25-2.25H6A2.25 2.25 0 003.75 6v8.25A2.25 2.25 0 006 16.5h2.25m8.25-8.25H11.812a2.25 2.25 0 00-2.25 2.25v8.25a2.25 2.25 0 002.25 2.25h6.188a2.25 2.25 0 002.25-2.25V8.25a2.25 2.25 0 00-2.25-2.25z" /></svg>';
                        btn.classList.remove('success');
                    }, 2000);
                });
            });
        });
    });
});
