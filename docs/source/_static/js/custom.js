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
document.addEventListener('DOMContentLoaded', function () {
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
            var overlay = document.getElementById('theme-switch-overlay');
            if (overlay) {
                overlay.style.setProperty('display', 'flex', 'important');
                overlay.classList.add('active');
            }

            // Force reflow
            void (overlay ? overlay.offsetHeight : 0);

            var startTime = Date.now();
            // Increase initial delay to 400ms to ensure spinner is animating smoothly before heavy hit
            setTimeout(function () {
                document.body.classList.toggle('dark-mode');
                document.documentElement.classList.toggle('dark-mode');
                toggleBtn.classList.toggle('active');
                localStorage.setItem('pylegend-theme', document.body.classList.contains('dark-mode') ? 'dark' : 'light');

                // Use double requestAnimationFrame to wait for the next frame after DOM updates
                requestAnimationFrame(function () {
                    requestAnimationFrame(function () {
                        var elapsed = Date.now() - startTime;
                        var remaining = Math.max(0, 3000 - elapsed);

                        setTimeout(function () {
                            if (overlay) {
                                overlay.classList.remove('active');
                                // Ensure it's hidden after fade out time
                                setTimeout(function () { overlay.style.display = 'none'; }, 300);
                            }
                        }, remaining);
                    });
                });
            }, 400);
        });
    }
});

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
